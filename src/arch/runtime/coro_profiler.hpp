// Copyright 2010-2012 RethinkDB, all rights reserved.
#ifndef ARCH_RUNTIME_CORO_PROFILER_HPP_
#define	ARCH_RUNTIME_CORO_PROFILER_HPP_


#ifdef ENABLE_CORO_PROFILER

#include <array>
#include <map>
#include <fstream>

#include "utils.hpp"
#include "arch/spinlock.hpp"
#include "config/args.hpp"

#define CORO_PROFILER_CALLTREE_DEPTH            10
#define CORO_PROFILER_REPORTING_INTERVAL        (secs_to_ticks(1) * 2)


/* 
 * The `coro_profiler_t` collects information about where coroutines spend time.
 * In order to turn it on, `ENABLE_CORO_PROFILER` must be defined at compile time.
 * It will only work reliably in debug mode.
 * 
 * The coro profiler records a sample whenever it encounters a `PROFILER_RECORD_SAMPLE`
 * and also every time a coroutine yields.
 * 
 * The following data is aggregated:
 *      - How often a certain recording point has been reached within the past
 *        `CORO_PROFILER_REPORTING_INTERVAL`.
 *      - How much time has passed since the coroutine has resumed running
 *        (this is useful to identify coroutines that run for long 
 *        periods of time without yielding control)
 *      - How much time has passed on a coroutine since the previous recording
 *        point
 * 
 * A combination of coro_type (signature of the function that spawned the coroutine)
 * and a limited-depth backtrace (compare `CORO_PROFILER_CALLTREE_DEPTH`) is used to
 * identify an "execution point". Data is recorded and reported for each such
 * execution point.
 * 
 * The aggregated data is written to the file "coro_profiler_out.py" in the working
 * directory. Data is written every `CORO_PROFILER_REPORTING_INTERVAL` ticks.
 */
class coro_profiler_t {
public:
    // Should you ever want to make this a true singleton, just make the
    // constructor private.
    coro_profiler_t();
    ~coro_profiler_t();
    
    static coro_profiler_t &get_global_profiler();
    
    void record_sample(size_t levels_to_strip_from_backtrace = 0);
    
    // coroutine execution is resumed
    void record_coro_resume();
    // coroutine execution yields
    void record_coro_yield(size_t levels_to_strip_from_backtrace);
    
private:
    typedef std::array<void *, CORO_PROFILER_CALLTREE_DEPTH> small_trace_t;
    // Identify an execution point of a coroutine by a pair of 
    // the coro's coroutine_type (the function which spawned it) and
    // a small_trace_t of its current execution point.
    typedef std::pair<std::string, small_trace_t> coro_execution_point_key_t;
    struct coro_sample_t {
        coro_sample_t(ticks_t _ticks_since_resume, ticks_t _ticks_since_previous) :
            ticks_since_resume(_ticks_since_resume), ticks_since_previous(_ticks_since_previous) { }
        ticks_t ticks_since_resume;
        ticks_t ticks_since_previous;
    };
    struct per_execution_point_samples_t {
        per_execution_point_samples_t() : num_samples_total(0) { }
        int num_samples_total;
        std::vector<coro_sample_t> samples;
    };
    struct per_thread_samples_t {
        per_thread_samples_t() : ticks_at_last_report(get_ticks()) { }
        std::map<coro_execution_point_key_t, per_execution_point_samples_t> per_execution_point_samples;
        spinlock_t spinlock;
        // This field is a duplicate of the global `ticks_at_last_report` in
        // `coro_profiler_t`. We copy it in each thread in order to avoid having
        // to lock and access the global field from different threads.
        ticks_t ticks_at_last_report;
    };
    
    struct per_execution_point_collected_report_t {
        per_execution_point_collected_report_t() :
            num_samples(0),
            total_time_since_previous(0.0),
            total_time_since_resume(0.0) {
        }
        double get_avg_time_since_previous() const {
            return total_time_since_previous / std::max(1.0, static_cast<double>(num_samples));
        }
        double get_avg_time_since_resume() const {
            return total_time_since_resume / std::max(1.0, static_cast<double>(num_samples));
        }
        void collect(const coro_sample_t &sample) {
            total_time_since_previous += ticks_to_secs(sample.ticks_since_previous);
            total_time_since_resume += ticks_to_secs(sample.ticks_since_resume);
            ++num_samples;
        }

        int num_samples;
        // TODO: Also report total count
        // TODO: Also calculate standard deviation, min, max, mean.
        //  That will require a bit of refactoring.
        double total_time_since_previous;
        double total_time_since_resume;
    };
    
    void generate_report();
    void print_to_console(const std::map<coro_execution_point_key_t, per_execution_point_collected_report_t> &execution_point_reports);
    void print_to_reql(const std::map<coro_execution_point_key_t, per_execution_point_collected_report_t> &execution_point_reports);
    void write_reql_header();
    std::string format_execution_point(const coro_execution_point_key_t &execution_point);
    const std::string &get_frame_description(void *addr);
    coro_execution_point_key_t get_current_execution_point(size_t levels_to_strip_from_backtrace);
    
    // Would be nice if we could use one_per_thread here. However
    // that makes the construction order tricky.
    std::array<cache_line_padded_t<per_thread_samples_t >, MAX_THREADS> per_thread_samples;
    
    ticks_t ticks_at_last_report;
    /* Locking order is always: 
     * 1. report_interval_spinlock
     * 2. per_thread_samples.spinlock in ascending order of thread num
     * You can safely skip some of the locks in this order.
     * Acquiring locks in different orders can dead-lock.
     */
    spinlock_t report_interval_spinlock;
    
    std::map<void *, std::string> frame_description_cache;
    
    std::ofstream reql_output_file;
    
    DISABLE_COPYING(coro_profiler_t);
};

// Short-cuts
//
// PROFILER_CORO_RESUME and PROFILER_CORO_YIELD are meant to be used in
// the internal coroutine implementation to notify the profiler about when a coroutine
// yields and resumes execution respectively.
//
// PROFILER_RECORD_SAMPLE on the other hand can be used throughout the code to
// increase the granularity of profiling. By default, the coro profiler collects
// data only when a coroutine yields (assuming that PROFILER_CORO_YIELD gets called).
// PROFILER_RECORD_SAMPLE adds an additional point for data collection in between
// such yields and can be used to "trace" execution times through different
// sections of a given piece of code.
#define PROFILER_RECORD_SAMPLE coro_profiler_t::get_global_profiler().record_sample();
#define PROFILER_CORO_RESUME coro_profiler_t::get_global_profiler().record_coro_resume();
#define PROFILER_CORO_YIELD(STRIP_FRAMES) coro_profiler_t::get_global_profiler().record_coro_yield(STRIP_FRAMES);

#else /* ENABLE_CORO_PROFILER */

// Short-cuts (no-ops for disabled coro profiler)
#define PROFILER_RECORD_SAMPLE {}
#define PROFILER_CORO_RESUME {}
#define PROFILER_CORO_YIELD(STRIP_FRAMES) {}

#endif /* not ENABLE_CORO_PROFILER */

#endif	/* ARCH_RUNTIME_CORO_PROFILER_HPP_ */

