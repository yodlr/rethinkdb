// Copyright 2010-2013 RethinkDB, all rights reserved.
#include <map>

#include "unittest/gtest.hpp"

#include "clustering/generic/raft_core.hpp"
#include "clustering/generic/raft_core.tcc"
#include "clustering/generic/raft_network.hpp"
#include "clustering/generic/raft_network.tcc"
#include "unittest/clustering_utils.hpp"
#include "unittest/unittest_utils.hpp"

namespace unittest {

/* `dummy_raft_state_t` is meant to be used as the `state_t` parameter to
`raft_member_t`, with the `change_t` parameter set to `uuid_u`. It just records all the
changes it receives and their order. */
class dummy_raft_state_t {
public:
    typedef uuid_u change_t;
    std::vector<uuid_u> state;
    void apply_change(const change_t &uuid) {
        state.push_back(uuid);
    }
    bool operator==(const dummy_raft_state_t &other) const {
        return state == other.state;
    }
    bool operator!=(const dummy_raft_state_t &other) const {
        return state != other.state;
    }
};
RDB_MAKE_SERIALIZABLE_1(dummy_raft_state_t, state);

typedef raft_member_t<dummy_raft_state_t> dummy_raft_member_t;

/* `dummy_raft_cluster_t` manages a collection of `dummy_raft_member_t`s. It handles
passing RPCs between them, and it can simulate crashes and netsplits. It periodically
automatically calls `check_invariants()` on its members. */
class dummy_raft_cluster_t {
public:
    /* An `alive` member is a `dummy_raft_member_t` that can communicate with other alive
    members. An `isolated` member is a `dummy_raft_member_t` that cannot communicate with
    any other members. A `dead` member is just a stored `raft_persistent_state_t`. */
    enum class live_t { alive, isolated, dead };

    static const char *show_live(live_t live) {
        switch (live) {
            case live_t::alive: return "ALIVE";
            case live_t::isolated: return "ISOLATED";
            case live_t::dead: return "DEAD";
            default: unreachable();
        }
    }

    /* The constructor starts a cluster of `num` alive members with the given initial
    state. */
    dummy_raft_cluster_t(
                size_t num,
                const dummy_raft_state_t &initial_state,
                std::vector<raft_member_id_t> *member_ids_out) :
            mailbox_manager(&connectivity_cluster, 'M'),
            connectivity_cluster_run(&connectivity_cluster, get_unittest_addresses(),
                peer_address_t(), ANY_PORT, 0),
            check_invariants_timer(100, [this]() {
                coro_t::spawn_sometime(std::bind(
                    &dummy_raft_cluster_t::check_invariants,
                    this,
                    auto_drainer_t::lock_t(&drainer)));
                })
    {
        raft_config_t initial_config;
        for (size_t i = 0; i < num; ++i) {
            raft_member_id_t member_id(generate_uuid());
            if (member_ids_out) {
                member_ids_out->push_back(member_id);
            }
            initial_config.voting_members.insert(member_id);
        }
        for (const raft_member_id_t &member_id : initial_config.voting_members) {
            add_member(
                member_id,
                raft_persistent_state_t<dummy_raft_state_t>::make_initial(
                    initial_state, initial_config));
        }
    }

    ~dummy_raft_cluster_t() {
        /* We could just let the destructors run, but then we'd have to worry about
        destructor order, so this is safer and clearer */
        for (const auto &pair : members) {
            set_live(pair.first, live_t::dead);
        }
    }

    /* `join()` adds a new member to the cluster. The caller is responsible for running a
    Raft transaction to modify the config to include the new member. */
    raft_member_id_t join() {
        raft_persistent_state_t<dummy_raft_state_t> init_state;
        bool found_init_state = false;
        for (const auto &pair : members) {
            if (pair.second->member_drainer.has()) {
                init_state = pair.second->member->get_raft()->get_state_for_init();
                found_init_state = true;
                break;
            }
        }
        guarantee(found_init_state, "Can't add a new node to a cluster with no living "
            "members.");
        raft_member_id_t member_id(generate_uuid());
        add_member(member_id, init_state);
#ifdef RAFT_DEBUG_LOGGING
        debugf("%s: newly created\n", show(member_id).c_str());
#endif
        return member_id;
    }

    live_t get_live(const raft_member_id_t &member_id) {
        member_info_t *i = members.at(member_id).get();
        if (i->connected) {
            guarantee(i->member.has());
            return live_t::alive;
        } else if (i->member.has()) {
            return live_t::isolated;
        } else {
            return live_t::dead;
        }
    }

    /* `set_live()` puts the given member into the given state. */
    void set_live(const raft_member_id_t &member_id, live_t live) {
#ifdef RAFT_DEBUG_LOGGING
        debugf("%s: state %s -> %s\n", show(member_id).c_str(),
            show_live(get_live(member_id)), show_live(live));
#endif /* RAFT_DEBUG_LOGGING */
        member_info_t *i = members.at(member_id).get();
        if (i->connected && live != live_t::alive) {
            for (const auto &pair : members) {
                if (pair.second->connected) {
                    pair.second->directory.delete_key(member_id);
                    i->directory.delete_key(pair.first);
                }
            }
            i->connected = false;
        }
        {
            if (i->member.has() && live == live_t::dead) {
                scoped_ptr_t<auto_drainer_t> dummy;
                std::swap(i->member_drainer, dummy);
                dummy.reset();
                i->member.reset();
            }
            if (!i->member.has() && live != live_t::dead) {
                i->member.init(new raft_networked_member_t<dummy_raft_state_t>(
                    member_id, &mailbox_manager, &i->directory, i, i->stored_state, ""));
                i->member_drainer.init(new auto_drainer_t);
            }
        }
        if (!i->connected && live == live_t::alive) {
            i->connected = true;
            for (const auto &pair : members) {
                if (pair.second->connected) {
                    pair.second->directory.set_key_no_equals(
                        member_id, i->member->get_business_card());
                    i->directory.set_key_no_equals(
                        pair.first, pair.second->member->get_business_card());
                }
            }
        }
    }

    /* Blocks until it finds a cluster member which is advertising itself as ready for
    changes, then returns that member's ID. */
    raft_member_id_t find_leader(signal_t *interruptor) {
        while (true) {
            for (const auto &pair : members) {
                if (pair.second->member_drainer.has() &&
                        pair.second->member->get_raft()->
                            get_readiness_for_change()->get()) {
                    return pair.first;
                }
            }
            signal_timer_t timer;
            timer.start(10);
            wait_interruptible(&timer, interruptor);
        }
    }

    raft_member_id_t find_leader(int timeout) {
        signal_timer_t timer;
        timer.start(timeout);
        try {
            return find_leader(&timer);
        } catch (const interrupted_exc_t &) {
            crash("find_leader() timed out");
        }
    }

    /* Tries to perform the given change on the member with the given ID. */
    bool try_change(raft_member_id_t id, const uuid_u &change,
            signal_t *interruptor) {
        bool res;
        run_on_member(id, [&](dummy_raft_member_t *member, signal_t *interruptor2) {
            res = false;
            if (member != nullptr) {
                /* `interruptor2` is only pulsed when the member is being destroyed, so
                it's safe to pass as the `hard_interruptor` parameter */
                try {
                    scoped_ptr_t<raft_member_t<dummy_raft_state_t>::change_token_t> tok;
                    {
                        raft_member_t<dummy_raft_state_t>::change_lock_t change_lock(
                            member, interruptor);
                        tok = member->propose_change(&change_lock, change, interruptor2);
                    }
                    if (!tok.has()) {
                        return;
                    }
                    wait_interruptible(tok->get_ready_signal(), interruptor);
                    res = tok->wait();
                } catch (const interrupted_exc_t &) {
                    if (interruptor2->is_pulsed()) {
                        throw;
                    }
                }
            }
        });
        if (interruptor->is_pulsed()) {
            throw interrupted_exc_t();
        }
        return res;
    }

    /* Like `try_change()` but for Raft configuration changes */
    bool try_config_change(raft_member_id_t id, const raft_config_t &new_config,
            signal_t *interruptor) {
        bool res;
        run_on_member(id, [&](dummy_raft_member_t *member, signal_t *interruptor2) {
            res = false;
            if (member != nullptr) {
                /* `interruptor2` is only pulsed when the member is being destroyed, so
                it's safe to pass as the `hard_interruptor` parameter */
                try {
                    scoped_ptr_t<raft_member_t<dummy_raft_state_t>::change_token_t> tok;
                    {
                        raft_member_t<dummy_raft_state_t>::change_lock_t change_lock(
                            member, interruptor);
                        tok = member->propose_config_change(
                            &change_lock, new_config, interruptor2);
                    }
                    if (!tok.has()) {
                        return;
                    }
                    wait_interruptible(tok->get_ready_signal(), interruptor);
                    res = tok->wait();
                } catch (const interrupted_exc_t &) {
                    if (interruptor2->is_pulsed()) {
                        throw;
                    }
                }
            }
        });
        if (interruptor->is_pulsed()) {
            throw interrupted_exc_t();
        }
        return res;
    }

    /* `get_all_member_ids()` returns the member IDs of all the members of the cluster,
    alive or dead.  */
    std::set<raft_member_id_t> get_all_member_ids() {
        std::set<raft_member_id_t> member_ids;
        for (const auto &pair : members) {
            member_ids.insert(pair.first);
        }
        return member_ids;
    }

    /* `run_on_member()` calls the given function for the `dummy_raft_member_t *` with
    the given ID. If the member is currently dead, it calls the function with a NULL
    pointer. */
    void run_on_member(
            const raft_member_id_t &member_id,
            const std::function<void(dummy_raft_member_t *, signal_t *)> &fun) {
        member_info_t *i = members.at(member_id).get();
        if (i->member_drainer.has()) {
            auto_drainer_t::lock_t keepalive = i->member_drainer->lock();
            try {
                fun(i->member->get_raft(), keepalive.get_drain_signal());
            } catch (const interrupted_exc_t &) {
                /* do nothing */
            }
        } else {
            cond_t non_interruptor;
            fun(nullptr, &non_interruptor);
        }
    }

private:
    class member_info_t :
        public raft_storage_interface_t<dummy_raft_state_t> {
    public:
        member_info_t() : connected(false) { }
        member_info_t(member_info_t &&) = default;
        member_info_t &operator=(member_info_t &&) = default;

        void write_persistent_state(
                const raft_persistent_state_t<dummy_raft_state_t> &persistent_state,
                signal_t *interruptor) {
            block(interruptor);
            stored_state = persistent_state;
            block(interruptor);
        }
        void block(signal_t *interruptor) {
            if (randint(10) != 0) {
                coro_t::yield();
            }
            if (randint(10) == 0) {
                signal_timer_t timer;
                timer.start(randint(30));
                wait_interruptible(&timer, interruptor);
            }
        }

        dummy_raft_cluster_t *parent;
        raft_member_id_t member_id;
        raft_persistent_state_t<dummy_raft_state_t> stored_state;
        watchable_map_var_t<raft_member_id_t, raft_business_card_t<dummy_raft_state_t> >
            directory;
        /* `connected` is `true` iff the member is alive. */
        bool connected;
        /* If the member is alive or isolated, `member` and `member_drainer` are set. */
        scoped_ptr_t<raft_networked_member_t<dummy_raft_state_t> > member;
        scoped_ptr_t<auto_drainer_t> member_drainer;
        
    };

    void add_member(
            const raft_member_id_t &member_id,
            raft_persistent_state_t<dummy_raft_state_t> initial_state) {
        scoped_ptr_t<member_info_t> i(new member_info_t);
        i->parent = this;
        i->member_id = member_id;
        i->stored_state = initial_state;
        members[member_id] = std::move(i);
        set_live(member_id, live_t::alive);
    }

    void check_invariants(UNUSED auto_drainer_t::lock_t keepalive) {
        std::set<dummy_raft_member_t *> member_ptrs;
        std::vector<auto_drainer_t::lock_t> keepalives;
        for (auto &pair : members) {
            if (pair.second->member_drainer.has()) {
                keepalives.push_back(pair.second->member_drainer->lock());
                member_ptrs.insert(pair.second->member->get_raft());
            }
        }
        dummy_raft_member_t::check_invariants(member_ptrs);
    }

    connectivity_cluster_t connectivity_cluster;
    mailbox_manager_t mailbox_manager;
    connectivity_cluster_t::run_t connectivity_cluster_run;

    std::map<raft_member_id_t, scoped_ptr_t<member_info_t> > members;
    auto_drainer_t drainer;
    repeating_timer_t check_invariants_timer;
};

/* `dummy_raft_traffic_generator_t` tries to send operations to the given Raft cluster at
a fixed rate. */
class dummy_raft_traffic_generator_t {
public:
    dummy_raft_traffic_generator_t(dummy_raft_cluster_t *_cluster, int num_threads) :
        cluster(_cluster) {
        for (int i = 0; i < num_threads; ++i) {
            coro_t::spawn_sometime(std::bind(
                &dummy_raft_traffic_generator_t::do_background_changes,
                this, drainer.lock()));
        }
    }

    size_t get_num_changes() {
        return committed_changes.size();
    }

    void check_changes_present(const dummy_raft_state_t &state) {
        std::set<uuid_u> all_changes;
        for (const uuid_u &change : state.state) {
            all_changes.insert(change);
        }
        for (const uuid_u &change : committed_changes) {
            ASSERT_EQ(1, all_changes.count(change));
        }
    }

    void do_changes(int count, int timeout_ms) {
#ifdef RAFT_DEBUG_LOGGING
        debugf("do_changes(): begin %d changes in %dms\n", count, timeout_ms);
        std::map<raft_member_id_t, int> leaders;
#endif
        int done = 0;
        try {
            signal_timer_t timer;
            timer.start(timeout_ms);
            while (done < count) {
                uuid_u change = generate_uuid();
                raft_member_id_t leader = cluster->find_leader(&timer);
                bool ok = cluster->try_change(leader, change, &timer);
                if (ok) {
#ifdef RAFT_DEBUG_LOGGING
                    ++leaders[leader];
#endif
                    committed_changes.insert(change);
                    ++done;
                }
            }
        } catch (const interrupted_exc_t &) {
            ADD_FAILURE() << "do_changes() only completed " << done << "/" << count <<
                " changes in " << timeout_ms << "ms";
        }
#ifdef RAFT_DEBUG_LOGGING
        std::string message;
        for (const auto &pair : leaders) {
            if (!message.empty()) {
                message += ", ";
            }
            message += strprintf("%s*%d", show(pair.first).c_str(), pair.second);
        }
        debugf("do_changes(): end changes %s\n", message.c_str());
#endif
    }

private:
    void do_background_changes(auto_drainer_t::lock_t keepalive) {
        try {
            while (true) {
                uuid_u change = generate_uuid();
                raft_member_id_t leader = cluster->find_leader(
                    keepalive.get_drain_signal());
                bool ok = cluster->try_change(
                    leader, change, keepalive.get_drain_signal());
                if (ok) {
                    committed_changes.insert(change);
                }
            }
        } catch (const interrupted_exc_t &) {
            /* We're shutting down. No action is necessary. */
        }
    }
    std::set<uuid_u> committed_changes;
    dummy_raft_cluster_t *cluster;
    auto_drainer_t drainer;
};

void do_writes(dummy_raft_cluster_t *cluster, raft_member_id_t leader, int ms, int expect) {
    dummy_raft_traffic_generator_t traffic_generator(cluster, 1);
    traffic_generator.do_changes(expect, ms);
    cluster->run_on_member(leader, [&](dummy_raft_member_t *member, signal_t *) {
        dummy_raft_state_t state = member->get_committed_state()->get().state;
        traffic_generator.check_changes_present(state);
    });
}

dummy_raft_cluster_t::live_t dead_or_isolated() {
    if (randint(2) == 0) {
        return dummy_raft_cluster_t::live_t::dead;
    } else {
        return dummy_raft_cluster_t::live_t::isolated;
    }
}

TPTEST(ClusteringRaft, Basic) {
    /* Spin up a Raft cluster and wait for it to elect a leader */
    dummy_raft_cluster_t cluster(5, dummy_raft_state_t(), nullptr);
    raft_member_id_t leader = cluster.find_leader(60000);
    /* Do some writes and check the result */
    do_writes(&cluster, leader, 2000, 30);
}

TPTEST(ClusteringRaft, Failover) {
    std::vector<raft_member_id_t> member_ids;
    dummy_raft_cluster_t cluster(5, dummy_raft_state_t(), &member_ids);
    dummy_raft_traffic_generator_t traffic_generator(&cluster, 3);
    raft_member_id_t leader = cluster.find_leader(60000);
    do_writes(&cluster, leader, 2000, 30);
    cluster.set_live(member_ids[0], dead_or_isolated());
    cluster.set_live(member_ids[1], dead_or_isolated());
    leader = cluster.find_leader(60000);
    do_writes(&cluster, leader, 2000, 30);
    cluster.set_live(member_ids[2], dead_or_isolated());
    cluster.set_live(member_ids[3], dead_or_isolated());
    cluster.set_live(member_ids[0], dummy_raft_cluster_t::live_t::alive);
    cluster.set_live(member_ids[1], dummy_raft_cluster_t::live_t::alive);
    leader = cluster.find_leader(60000);
    do_writes(&cluster, leader, 2000, 30);
    cluster.set_live(member_ids[4], dead_or_isolated());
    cluster.set_live(member_ids[2], dummy_raft_cluster_t::live_t::alive);
    cluster.set_live(member_ids[3], dummy_raft_cluster_t::live_t::alive);
    leader = cluster.find_leader(60000);
    do_writes(&cluster, leader, 2000, 30);
    ASSERT_LT(100, traffic_generator.get_num_changes());
    cluster.run_on_member(leader, [&](dummy_raft_member_t *member, signal_t *) {
        dummy_raft_state_t state = member->get_committed_state()->get().state;
        traffic_generator.check_changes_present(state);
    });
}

TPTEST(ClusteringRaft, MemberChange) {
    std::vector<raft_member_id_t> member_ids;
    size_t cluster_size = 5;
    dummy_raft_cluster_t cluster(cluster_size, dummy_raft_state_t(), &member_ids);
    dummy_raft_traffic_generator_t traffic_generator(&cluster, 3);
    for (size_t i = 0; i < 10; ++i) {
        /* Do some test writes */
        raft_member_id_t leader = cluster.find_leader(10000);
        do_writes(&cluster, leader, 2000, 10);

        /* Kill one member and do some more test writes */
        cluster.set_live(member_ids[i], dummy_raft_cluster_t::live_t::dead);
        leader = cluster.find_leader(10000);
        do_writes(&cluster, leader, 2000, 10);

        /* Add a replacement member and do some more test writes */
        member_ids.push_back(cluster.join());
        do_writes(&cluster, leader, 2000, 10);

        /* Update the configuration and do some more test writes */
        raft_config_t new_config;
        for (size_t n = i+1; n < i+1+cluster_size; ++n) {
            new_config.voting_members.insert(member_ids[n]);
        }
        signal_timer_t timeout;
        timeout.start(10000);
        cluster.try_config_change(leader, new_config, &timeout);
        do_writes(&cluster, leader, 2000, 10);
    }
    ASSERT_LT(100, traffic_generator.get_num_changes());
}

}   /* namespace unittest */

