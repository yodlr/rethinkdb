// Copyright 2010-2015 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_BACKTRACE_HPP_
#define RDB_PROTOCOL_BACKTRACE_HPP_

#include <vector>
#include <stdexcept>

#include "rdb_protocol/datum.hpp"
#include "rdb_protocol/error.hpp"
#include "rdb_protocol/ql2.pb.h"
#include "rdb_protocol/ql2_extensions.pb.h"

namespace ql {

class backtrace_registry_t;

// A query-language exception with its backtrace resolved to a datum_t
// This should only be thrown from outside term evaluation - it is only meant
// to be constructed or caught on the query's home coroutine.
class bt_exc_t : public std::exception {
public:
    bt_exc_t(Response::ResponseType _response_type,
             const std::string &_message,
             datum_t _bt_datum)
        : response_type(_response_type), message(_message), bt_datum(_bt_datum) { }
    virtual ~bt_exc_t() throw () { }

    const char *what() const throw () { return message.c_str(); }

    const Response::ResponseType response_type;
    const std::string message;
    const datum_t bt_datum;
};

// Used by the minidriver to replace nested backtraces in minidriver terms with
// the proper backtraces when copying args/optargs into minidriver terms.
class backtrace_patch_t : public intrusive_list_node_t<backtrace_patch_t> {
public:
    backtrace_patch_t(backtrace_id_t _parent_bt, backtrace_registry_t *_bt_reg);
    virtual ~backtrace_patch_t();

    bool get_patch(const Term *t, backtrace_id_t *bt_out) const;
    void add_patch(const Term *t, const ql::datum_t &val);

private:
    backtrace_registry_t *bt_reg;
    backtrace_id_t parent_bt;
    std::map<const Term *, backtrace_id_t> patches;
};

// Manages the lifetime of a backtrace_patch_t being applied to a backtrace_registry_t.
// The backtrace_patch_t should continue to exist until after the scope is destroyed.
class backtrace_patch_scope_t {
public:
    backtrace_patch_scope_t(backtrace_registry_t *_bt_reg,
                            const backtrace_patch_t *_patch);
    ~backtrace_patch_scope_t();

private:
    backtrace_registry_t *bt_reg;
    backtrace_patch_t *patch;
};

class backtrace_registry_t {
public:
    virtual ~backtrace_registry_t() { }

    virtual backtrace_id_t new_frame(backtrace_id_t parent_bt,
                                     const Term *t,
                                     const datum_t &val) = 0;

    static const datum_t EMPTY_BACKTRACE;

    bool check_for_patch(const Term *t,
                         backtrace_id_t *bt_out) const;

private:
    friend class backtrace_patch_scope_t;
    intrusive_list_t<backtrace_patch_t> patches;
};

// All backtrace ids allocated through this object will be the same as the backtrace id
// it was originally constructed with.  This is used when compiling minidriver functions
// or rewrites that have no relation to the main term tree.  The only exception is if
// a patch exists for the given term.
class dummy_backtrace_registry_t : public backtrace_registry_t {
public:
    dummy_backtrace_registry_t(backtrace_id_t _original_bt) :
        original_bt(_original_bt) { }
    virtual ~dummy_backtrace_registry_t() { }

    backtrace_id_t new_frame(backtrace_id_t parent_bt,
                             const Term *t,
                             const datum_t &val);

private:
    backtrace_id_t original_bt;
};

class real_backtrace_registry_t : public backtrace_registry_t {
public:
    real_backtrace_registry_t();
    real_backtrace_registry_t(real_backtrace_registry_t &&other) :
        frames(std::move(other.frames)) { }
    virtual ~real_backtrace_registry_t() { }

    backtrace_id_t new_frame(backtrace_id_t parent_bt,
                             const Term *t,
                             const datum_t &val);

    datum_t datum_backtrace(const exc_t &ex) const;

private:
    struct frame_t {
        frame_t(); // Only for creating the HEAD term
        frame_t(backtrace_id_t _parent, datum_t _val);
        bool is_head() const;

        backtrace_id_t parent;
        datum_t val;
    };

    std::vector<frame_t> frames;
    DISABLE_COPYING(real_backtrace_registry_t);
};

void fill_backtrace(Backtrace *bt_out,
                    datum_t backtrace);

void fill_error(Response *res_out,
                Response::ResponseType type,
                const std::string &message,
                datum_t backtrace);

} // namespace ql

#endif // RDB_PROTOCOL_BACKTRACE_HPP_
