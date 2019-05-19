#include <iostream>
#include "KiWi.hpp"
#ifdef HAVE_FOLLY
#include <folly/ConcurrentSkipList.h>
#else
#endif

int main() {
#if defined(HAVE_FOLLY) || defined(HAVE_GALOIS)
    std::unique_ptr<Chunk<int>> first_chunk = std::make_unique<Chunk<int>>(KeyCell<int>(), nullptr);
    std::unique_ptr<KiWi<int>> k = std::make_unique<KiWi<int>>(first_chunk.get(), true);
#else
#error not supported
#endif
    int iters = 8;

    for (int i = 1; i < iters; ++i) {
        k->put(KeyCell<int>(i,0,0,0),50);
    }

    for (int i = 1; i < iters; ++i) {
        KeyCell<int> kc = k->dequeue();
        if (!kc.m_is_valid) {
            throw std::logic_error("got invalid keycell");
        }
        int val = *kc.getM_key();
        if (val != i) {
            throw std::logic_error("got invalid keycelll");
        }
    }

    return 0;
}
