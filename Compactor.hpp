//
// Created by dvir on 07/02/19.
//

#ifndef KIWI_CPP_PQ_PORT_COMPACTOR_HPP
#define KIWI_CPP_PQ_PORT_COMPACTOR_HPP

#include "Chunk.hpp"
#include "MultiChunkIterator.hpp"

static inline int LOW_THRESHOLD()
{
    return g_chunk_size / 2;
}

static inline int HIGH_THRESHOLD()
{
    return g_chunk_size - MAX_THREADS;
}

static inline int MAX_RANGE_TO_APPEND()
{
    return 0.2 * g_chunk_size;
}

template<typename K>
class Compactor {
public:
    Chunk<K>* lastCheckedForAppend = nullptr;

    std::vector<Chunk<K>*>* compact(std::vector<Chunk<K>*>* frozenChunks, ScanIndex<K> scanIndex);

    bool canAppendSuffix(int key_index, std::vector<Chunk<K>*>* frozenSuffix, int maxCount);

    void completeCopy(Chunk<K>* dest, int key_index, std::vector<Chunk<K>*>* srcChunks, ScanIndex<K> scanIndex);
};


#include "Compactor.cpp"

#endif //KIWI_CPP_PQ_PORT_COMPACTOR_HPP
