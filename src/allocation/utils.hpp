#ifndef ALLOCATION_UTILS_HPP
#define ALLOCATION_UTILS_HPP

#include "allocation/traits.hpp"

namespace allocation {

template <class T, class Allocator, class... Args>
T* make(std::allocator_arg_t, Allocator &alloc, Args&&... args) {
    typedef typename traits<Allocator>::type traits;
    T* result = traits::allocate(alloc, 1);
    try {
        traits::construct(alloc, result, std::forward<Args>(args)...);
        return result;
    } catch(...) {
        traits::deallocate(alloc, result, 1);
        throw;
    }
}

}; // namespace allocation

#endif // ALLOCATION_UTILS_HPP
