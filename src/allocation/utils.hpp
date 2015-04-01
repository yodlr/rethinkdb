#ifndef ALLOCATION_UTILS_HPP
#define ALLOCATION_UTILS_HPP

#include <boost/container/allocator_traits.hpp>

template <class T, class Allocator, class... Args>
T* make(std::allocator_arg_t, Allocator &alloc, Args&&... args) {
    T* result = boost::container::allocator_traits<Allocator>::allocate(alloc, 1);
    try {
        boost::container::allocator_traits<Allocator>::construct
            (alloc, result, std::forward<Args>(args)...);
        return result;
    } catch(...) {
        boost::container::allocator_traits<Allocator>::deallocate(alloc, result, 1);
        throw;
    }
}

#endif // ALLOCATION_UTILS_HPP
