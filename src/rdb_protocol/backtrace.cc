#include "rdb_protocol/backtrace.hpp"

#include <algorithm>

namespace ql {

backtrace_patch_t::backtrace_patch_t(backtrace_id_t _parent_bt,
                                     backtrace_registry_t *_bt_reg) :
        bt_reg(_bt_reg), parent_bt(_parent_bt) {
    guarantee(bt_reg != nullptr);
}

backtrace_patch_t::~backtrace_patch_t() { }

bool backtrace_patch_t::get_patch(const Term *t, backtrace_id_t *bt_out) const {
    auto const &it = patches.find(t);
    if (it != patches.end()) {
        *bt_out = it->second;
    }
    return it != patches.end();
}

void backtrace_patch_t::add_patch(const Term *t, const datum_t &val) {
    backtrace_id_t bt = bt_reg->new_frame(parent_bt, t, val);
    patches[t] = bt;
}

backtrace_patch_scope_t::backtrace_patch_scope_t(backtrace_registry_t *_bt_reg,
                                                 const backtrace_patch_t *_patch) :
        bt_reg(_bt_reg), patch(const_cast<backtrace_patch_t *>(_patch)) {
    bt_reg->patches.push_back(patch);
}

backtrace_patch_scope_t::~backtrace_patch_scope_t() {
    bt_reg->patches.remove(patch);
}

const datum_t backtrace_registry_t::EMPTY_BACKTRACE = datum_t::empty_array();

bool backtrace_registry_t::check_for_patch(const Term *t,
                                           backtrace_id_t *bt_out) const {
    for (auto p = patches.head(); p != nullptr; p = patches.next(p)) {
        if (p->get_patch(t, bt_out)) {
            return true;
        }
    }
    return false;
}

backtrace_id_t dummy_backtrace_registry_t::new_frame(backtrace_id_t,
                                                     const Term *t,
                                                     const datum_t &) {
    backtrace_id_t res;
    if (!check_for_patch(t, &res)) {
        res = original_bt;
    }
    return res;
}

real_backtrace_registry_t::frame_t::frame_t(backtrace_id_t _parent, datum_t _val) :
    parent(_parent), val(_val) { }

bool real_backtrace_registry_t::frame_t::is_head() const {
    return val.get_type() == datum_t::type_t::R_NULL;
}

real_backtrace_registry_t::real_backtrace_registry_t() {
    frames.emplace_back(backtrace_id_t::empty(), datum_t::null());
}

backtrace_id_t real_backtrace_registry_t::new_frame(backtrace_id_t parent_bt,
                                                    const Term *t,
                                                    const datum_t &val) {
    backtrace_id_t res;
    if (!check_for_patch(t, &res)) {
        frames.emplace_back(parent_bt, val);
        res = backtrace_id_t(frames.size() - 1);
    }
    return res;
}

datum_t real_backtrace_registry_t::datum_backtrace(const exc_t &ex) const {
    size_t dummy_frames = ex.dummy_frames();
    r_sanity_check(ex.backtrace().get() < frames.size());
    std::vector<datum_t> res;
    for (const frame_t *f = &frames[ex.backtrace().get()];
         !f->is_head(); f = &frames[f->parent.get()]) {
        r_sanity_check(f->parent.get() < frames.size());
        if (dummy_frames > 0) {
            --dummy_frames;
        } else {
            res.push_back(f->val);
        }
    }
    std::reverse(res.begin(), res.end());
    return datum_t(std::move(res), configured_limits_t::unlimited);
}

void fill_backtrace(Backtrace *bt_out,
                    ql::datum_t bt_datum) {
    for (size_t i = 0; i < bt_datum.arr_size(); ++i) {
        datum_t frame = bt_datum.get(i);
        Frame *pb_frame = bt_out->add_frames();
        if (frame.get_type() == datum_t::type_t::R_STR) {
            pb_frame->set_type(Frame::OPT);
            pb_frame->set_opt(frame.as_str().to_std());
        } else if (frame.get_type() == datum_t::type_t::R_NUM) {
            pb_frame->set_type(Frame::POS);
            pb_frame->set_pos(frame.as_int());
        } else {
            unreachable();
        }
    }
}

void fill_error(Response *res,
                Response::ResponseType type,
                const std::string &message,
                datum_t backtrace) {
    guarantee(type == Response::CLIENT_ERROR ||
              type == Response::COMPILE_ERROR ||
              type == Response::RUNTIME_ERROR);
    Datum error_msg;
    error_msg.set_type(Datum::R_STR);
    error_msg.set_r_str(message);
    res->set_type(type);
    res->clear_response();
    res->clear_profile();
    *res->add_response() = error_msg;
    fill_backtrace(res->mutable_backtrace(), backtrace);
}

}
