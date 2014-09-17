// Copyright 2010-2014 RethinkDB, all rights reserved.
#ifndef CLUSTERING_GENERIC_RAFT_CORE_TCC_
#define CLUSTERING_GENERIC_RAFT_CORE_TCC_

template<class state_t, class change_t>
void raft_member_t<state_t, change_t>::on_append_entries_rpc(
        raft_term_t term,
        const raft_member_id_t &leader_id,
        const raft_log_t<change_t> &entries,
        raft_log_index_t leader_commit,
        signal_t *interruptor,
        raft_term_t *term_out,
        raft_change_outcome_t *success_out) {
    assert_thread();
    mutex_t::acq_t mutex_acq(&mutex);

    /* Raft paper, Figure 2: If RPC request or response contains term T > currentTerm:
    set currentTerm = T, convert to follower */
    if (term > ps.current_term) {
        update_term(term, &mutex_acq);
        if (mode != mode_t::follower) {
            become_follower(&mutex_acq);
        }
        /* Continue processing the RPC as follower */
    }

    /* Raft paper, Figure 2: "Reply false if term < currentTerm (SE 5.1)"
    Raft paper, Section 5.1: "If a server receives a request with a stale term number, it
    rejects the request" */
    if (term < ps.current_term) {
        /* Raft paper, Figure 2: term should be set to "currentTerm, for leader to update
        itself" */
        *term_out = ps.current_term;
        *success_out = raft_change_outcome_t::retry;
        return;
    }

    guarantee(term == ps.current_term);   /* sanity check */

    /* Raft paper, Section 5.2: "While waiting for votes, a candidate may receive an
    AppendEntries RPC from another server claiming to be leader. If the leader's term
    (included in its RPC) is at least as large as the candidate's current term, then the
    candidate recognizes the leader as legitimate and returns to follower state. If the
    term in the RPC is smaller than the candidate's current term, then the candidate
    rejects the RPC and continues in candidate state." */
    if (mode == mode_t::candidate) {
        become_follower();
    }

    /* Raft paper, Section 5.2: "at most one candidate can win the election for a
    particular term"
    If we're leader, then we won the election, so it makes no sense for us to receive an
    RPC from another member that thinks it's leader. */
    guarantee(mode != mode_t::leader);

    /* See if all of the proposed changes are acceptable. This is not part of the
    original Raft algorithm. We don't bother checking changes that are earlier than
    `leader_commit` because they have already been committed, so our rejection cannot
    make a difference (and it would cause the algorithm to lock up). */
    for (raft_log_index_t i = min(entries.get_latest_index(), leader_commit) + 1;
            i <= entries.get_latest_index();
            ++i) {
        if (!interface->consider_proposed_change(
                entries.get_entry(i).first, interruptor)) {
            /* If one of the changes is rejected, we bail out immediately and don't touch
            anything, to minimize the probability of introducing a bug in the Raft
            algorithm. */
            *term_out = current_term;
            *success_out = raft_change_outcome_t::rejected;
            return;
        }
    }

    /* Raft paper, Figure 2: "Reply false if log doesn't contain an entry at prevLogIndex
    whose term matches prevLogTerm" */
    if (entries.prev_log_index > ps.log.get_latest_index() ||
            ps.log.get_entry_term(prev_log_index) != entries.prev_log_term) {
        *term_out = current_term;
        *success_out = raft_change_outcome_t::retry;
        return;
    }

    /* Raft paper, Figure 2: "If an existing entry conflicts with a new one (same index
    but different terms), delete the existing entry and all that follow it" */
    for (raft_log_index_t i = entries.prev_log_index + 1;
            i <= min(ps.log.get_latest_index(), entries.get_latest_index());
            ++i) {
        if (log.get_entry_term(i) != entries.get_entry_term(i)) {
            log.delete_entries_from(i);
            break;
        }
    }

    /* Raft paper, Figure 2: "Append any new entries not already in the log" */
    for (raft_log_index_t i = ps.log.get_latest_index() + 1;
            i <= entries.get_latest_index();
            ++i) {
        ps.log.append(entries.get_entry(i));
    }

    /* Raft paper, Figure 2: "If leaderCommit > commitIndex, set commitIndex = min(
    leaderCommit, index of last new entry)" */
    while (leader_commit > commit_index) {
        update_commit_index(min(leader_commit, entries.get_latest_index()), &mutex_acq);
    }

    /* Recall that `this_term_leader_id` is set to `nil_uuid()` if we haven't seen a
    leader yet this term. */
    if (this_term_leader_id.is_nil()) {
        this_term_leader_id = leader_id;
    } else {
        /* Raft paper, Section 5.2: "at most one candidate can win the election for a
        particular term" */
        guarantee(this_term_leader_id == leader_id);
    }

    /* Raft paper, Figure 2: "Persistent state [is] updated on stable storage before
    responding to RPCs" */
    interface->write_persistent_state(ps, interruptor);

    *term_out = current_term;
    *success_out = raft_change_outcome_t::success;
}


template<class state_t, class change_t>
void raft_member_t<state_t, change_t>::on_request_vote(
        raft_term_t term,
        const raft_member_id_t &candidate_id,
        raft_log_index_t last_log_index,
        raft_term_t last_log_term,
        signal_t *interruptor,
        raft_term_t *term_out,
        bool *vote_granted_out) {
    assert_thread();
    mutex_t::acq_t mutex_acq(&mutex);

    /* Raft paper, Figure 2: If RPC request or response contains term T > currentTerm:
    set currentTerm = T, convert to follower */
    if (term > ps.current_term) {
        update_term(term, &mutex_acq);
        if (mode != mode_t::follower) {
            become_follower(&mutex_acq);
        }
        /* Continue processing the RPC as follower */
    }

    /* Raft paper, Figure 2: "Reply false if term < currentTerm" */
    if (term < ps.current_term) {
        *term_out = ps.current_term;
        *vote_granted_out = false;
        return;
    }

    /* Sanity checks, not explicitly described in the Raft paper. */
    guarantee(candidate_id != member_id, "We shouldn't be requesting a vote from "
        "ourself.");
    if (mode != mode_t::follower) {
        guarantee(voted_for == member_id, "We should have voted for ourself already.");
    }

    /* Raft paper, Figure 2: "If votedFor is null or candidateId, and candidate's log is
    at least as up-to-date as receiver's log, grant vote */

    /* So if `voted_for` is neither `nil_uuid()` nor `candidate_id`, we don't grant the
    vote */
    if (!ps.voted_for.is_nil() && ps.voted_for != candidate_id) {
        *term_out = ps.current_term;
        *vote_granted_out = false;
        return;
    }

    /* Raft paper, Section 5.4.1: "Raft determines which of two logs is more up-to-date
    by comparing the index and term of the last entries in the logs. If the logs have
    last entries with different terms, then the log with the later term is more
    up-to-date. If the logs end with the same term, then whichever log is longer is more
    up-to-date." */
    bool candidate_is_at_least_as_up_to_date =
        last_log_term > ps.log.get_entry_term(ps.log.get_latest_index()) ||
            (last_log_term == ps.log.get_entry_term(ps.log.get_latest_index()) &&
                last_log_index >= ps.log.get_latest_index());
    if (!candidate_is_at_least_as_up_to_date) {
        *term_out = ps.current_term;
        *vote_granted_out = false;
        return;
    }

    ps.voted_for = candidate_id;

    /* Raft paper, Figure 2: "Persistent state [is] updated on stable storage before
    responding to RPCs" */
    interface->write_persistent_state(ps, interruptor);

    *term_out = ps.current_term;
    *vote_granted_out = true;
}

template<class state_t, class change_t>
void raft_member_t<state_t, change_t>::on_install_snapshot(
        raft_term_t term,
        const raft_member_id_t &leader_id,
        raft_log_index_t last_included_index,
        raft_term_t last_included_term,
        const state_t &snapshot,
        signal_t *interruptor,
        raft_term_t *term_out) {
    assert_thread();
    mutex_t::acq_t mutex_acq(&mutex);

    /* Raft paper, Figure 2: If RPC request or response contains term T > currentTerm:
    set currentTerm = T, convert to follower */
    if (term > ps.current_term) {
        update_term(term, &mutex_acq);
        if (mode != mode_t::follower) {
            become_follower(&mutex_acq);
        }
        /* Continue processing the RPC as follower */
    }

    /* Raft paper, Figure 13: "Reply immediately if term < currentTerm" */
    if (term < ps.current_term) {
        *term_out = ps.current_term;
        return;
    }

    /* Raft paper, Figure 13: "Save snapshot file"
    Remember that `log.prev_log_index` and `log.prev_log_term` correspond to the snapshot
    metadata. */
    ps.snapshot = snapshot;
    ps.log.prev_log_index = last_included_index;
    ps.log.prev_log_term = last_included_term;

    /* Raft paper, Figure 13: "If existing log entry has same index and term as
    snapshot's last included entry, retain log entries following it and reply" */
    if (last_included_index <= ps.log.prev_log_index) {
        /* The proposed snapshot starts at or before our current snapshot. It's
        impossible to check if an existing log entry has the same index and term because
        the snapshot's last included entry is before our most recent entry. But if that's
        the case, we don't need this snapshot, so we can safely ignore it. */
        *term_out = ps.current_term;
        return;
    } else if (last_included_index <= ps.log.get_latest_index() &&
            ps.log.get_entry_term(last_included_index) == last_included_term) {
        *term_out = ps.current_term;
        return;
    }

    /* Raft paper, Figure 13: "Discard the entire log" */
    ps.log.entries.clear();

    /* Raft paper, Figure 13: "Reset state machine using snapshot contents" */
    state_machine = ps.snapshot;
    commit_index = last_applied = ps.log.prev_log_index;

    /* Recall that `this_term_leader_id` is set to `nil_uuid()` if we haven't seen a
    leader yet this term. */
    if (this_term_leader_id.is_nil()) {
        this_term_leader_id = leader_id;
    } else {
        /* Raft paper, Section 5.2: "at most one candidate can win the election for a
        particular term" */
        guarantee(this_term_leader_id == leader_id);
    }

    /* Raft paper, Figure 2: "Persistent state [is] updated on stable storage before
    responding to RPCs" */
    interface->write_persistent_state(ps, interruptor);

    *term_out = ps.current_term;
}

template<class state_t, class change_t>
void raft_member_t<state_t, change_t>::update_term(
        raft_term_t new_term, const mutex_t::acq_t *mutex_acq) {
    mutex_acq->assert_is_holding(&mutex);

    guarantee(new_term > ps.current_term);
    ps.current_term = new_term;

    /* In Figure 2, `votedFor` is defined as "candidateId that received vote in
    current term (or null if none)". So when the current term changes, we have to
    update `voted_for`. */
    ps.voted_for = nil_uuid();

    /* The same logic applies to `this_term_leader_id`. */
    this_term_leader_id = nil_uuid();

    /* RSI: Ensure that we flush to stable storage, because we updated `ps.voted_for` and
    `ps.current_term` */
}

template<class state_t, class change_t>
void raft_member_t<state_t, change_t>::update_commit_index(
        raft_log_index_t new_commit_index, const mutex_t::acq_t *mutex_acq) {
    mutex_acq->assert_is_holding(&mutex);

    guarantee(new_commit_index > commit_index);
    commit_index = new_commit_index;

    /* Raft paper, Figure 2: "If commitIndex > lastApplied: increment lastApplied, apply
    log[lastApplied] to state machine" */
    while (last_applied < commit_index) {
        ++last_applied;
        guarantee(state_machine.consider_change(ps.log.get_entry(last_applied).first),
            "We somehow committed a change that's not valid for the state.");
        state_machine.apply_change(ps.log.get_entry(last_applied).first);
    }

    /* Take a snapshot as described in Section 7. We can snapshot any time we like; this
    implementation currently snapshots after every change. If the `state_t` ever becomes
    large enough to justify a different behavior, we could wait before snapshotting. */
    if (last_applied > ps.log.prev_log_index) {
        ps.snapshot = state_machine;
        /* This automatically updates `ps.log.prev_log_index` and `ps.log.prev_log_term`,
        which are equivalent to the "last included index" and "last included term"
        described in Section 7 of the Raft paper. */
        ps.log.delete_entries_to(last_applied);
    }

    /* RSI: Ensure that we flush to stable storage. */
}

template<class state_t, class change_t>
void raft_member_t<state_t, change_t>::become_follower(const mutex_t::acq_t *mutex_acq) {
    mutex_acq->assert_is_holding(&mutex);
    guarantee(mode == mode_t::candidate || mode == mode_t::leader);
    guarantee(drainer.has());

    /* This will interrupt `leader_coro()` and block until it exits */
    drainer.reset();

    /* `leader_coro()` should have reset `mode` when it exited */
    guarantee(mode == mode_t::follower);
}

template<class state_t, class change_t>
void raft_member_t<state_t, change_t>::become_candidate(
        const mutex_t::acq_t *mutex_acq) {
    mutex_acq->assert_is_holding(&mutex);
    guarantee(mode == mode_t::follower);
    guarantee(!drainer.has())
    drainer.init(new auto_drainer_t);
    cond_t pulse_when_done_with_setup;
    coro_t::spawn_sometime(boost::bind(&raft_member_t::leader_coro, this,
        mutex_acq, &pulse_when_done_with_setup, auto_drainer_t::lock_t(drainer.get())));
}

template<class state_t, class change_t>
void raft_member_t<state_t, change_t>::leader_coro(
        const mutex_t::acq_t *mutex_acq_for_setup,
        cond_t *pulse_when_done_with_setup,        
        auto_drainer_t::lock_t keepalive) {
    guarantee(mode == mode_t::follower);
    keepalive.assert_is_holding(drainer.get());
    mutex_acq->assert_is_holding(&mutex);

    /* Raft paper, Section 7: "To begin an election, a follower increments its current
    term and transitions to candidate state." */
    update_term(ps.current_term + 1, mutex_acq_for_setup);
    mode = mode_t::candidate;

    /* Raft paper, Section 7: "It then votes for itself." */
    std::set<raft_member_id_t> votes;
    ps.voted_for = member_id;
    votes.insert(member_id);

    std::set<raft_member_id_t> peers =
        get_bleeding_edge_state(mutex_acq_for_setup).get_all_members();

    /* Now that we're done with the initial state updates, we can let
    `become_candidate()` return. */
    pulse_when_done_with_setup->pulse();

    try {
        /* Raft paper, Section 7: "[The candidate] issues RequestVote RPCs in parallel to
        each of the other servers in the cluster." */
        pmap(peers.begin(), peers.end(), [&](const raft_member_id_t &peer) {
            raft_term_t term_out;
            bool vote_granted_out;
            try {
                interface->send_request_vote_rpc(
                    peer,
                    ps.current_term,
                    member_id,
                    ps.log.get_latest_index(),
                    ps.log.get_entry_term(ps.log.get_latest_index()),
                    keepalive.get_drain_signal(),
                    &term_out,
                    &vote_granted_out);
            } catch (interrupted_exc_t) {
                /* We can't throw `interrupted_exc_t` inside of `pmap()`, but we'll
                re-throw after the `pmap()` returns. */
                return;
            }
        });
        if (keepalive.get_drain_signal()->is_pulsed()) {
            throw interrupted_exc_t();
        }
    } catch (interrupted_exc_t) {
        mode = mode_t::follower;
    }
}

template<class state_t, class change_t>
state_t raft_member_t<state_t, change_t>::get_state_including_log(
        const mutex_t::acq_t *mutex_acq) {
    mutex_acq->assert_is_holding(&mutex);
    state_t s = state_machine;
    for (raft_log_index_t i = last_applied+1; i <= ps.log.get_latest_index(); ++i) {
        guarantee(s.consider_change(ps.log.get_entry(i).first),
            "We somehow got a change that's not valid for the state.");
        s.apply_change(ps.log.get_entry(i).first);
    }
    return s;
}

#endif /* CLUSTERING_GENERIC_RAFT_CORE_TCC_ */
