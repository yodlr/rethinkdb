#ifndef ALLOCATION_UTILS_HPP
#define ALLOCATION_UTILS_HPP

#include "allocation/traits.hpp"

namespace allocation {

template <class T, class Allocator, class... Args>
T* make(std::allocator_arg_t, Allocator &alloc, Args&&... args) {
    T* result = allocator_traits<Allocator>::allocate(alloc, 1);
    try {
        allocator_traits<Allocator>::construct(alloc, result,
                                               std::forward<Args>(args)...);
        return result;
    } catch(...) {
        allocator_traits<Allocator>::deallocate(alloc, result, 1);
        throw;
    }
}

}; // namespace allocation

#endif // ALLOCATION_UTILS_HPP
