// Copyright 2015 RethinkDB, all rights reserved.
#ifndef ALLOCATION_TRAITS_HPP_
#define ALLOCATION_TRAITS_HPP_

// Necessary because we can't rely on `std::allocator_traits` (not
// there on gcc 4.6) and Boost 1.49 is the first to define an
// `allocator_traits` type which we can use--and then promptly moved
// the include file in 1.50.
#ifdef BOOST_NO_STD_ALLOCATOR
#if BOOST_VERSION < 104900
#error Need Boost 1.49.0 for allocator traits
#elif BOOST_VERSION < 105000
#include <boost/container/allocator/allocator_traits.hpp>
#else
#include <boost/container/allocator_traits.hpp>
#endif

namespace allocation {
    template<typename T> using allocator_traits = boost::container::allocator_traits<T>;
}; // namespace allocation

#else
#include <memory>

namespace allocation {
    template<typename T> using allocator_traits = std::allocator_traits<T>;
}; // namespace allocation

#endif

#endif  // ALLOCATION_TRAITS_HPP_
