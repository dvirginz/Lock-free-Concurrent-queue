//
// Created by dvir on 06/02/19.
//

#ifndef KIWI_CPP_PQ_PORT_REBALANCER_HPP
#define KIWI_CPP_PQ_PORT_REBALANCER_HPP

#define MAX_AFTER_MERGE_PART 0.5

#include <cmath>
#include "Chunk.hpp"

template<typename K>
class KiWi;

template<typename K>
class Rebalancer : public std::enable_shared_from_this<Rebalancer<K>>{
public:
    std::atomic<Chunk<K>*> m_nextToEngage;
    Chunk<K>* m_startChunk;
    KiWi<K>* m_chunkIterator;

    //todo understand why this is atomic, std::vector is thread safe if we read/write to different objects in the vector
    std::atomic<std::vector<Chunk<K>*>*> m_compactedChunks;
    std::atomic<std::vector<Chunk<K>*>*> m_engagedChunks;
    std::atomic<bool> m_freezedItems;

    // assumption -- chunk once engaged remains with the same rebalance object forever, till GC
    std::shared_ptr<Rebalancer<K>> engageChunks();

    std::vector<Chunk<K>*>* createEngagedList(Chunk<K>* firstChunkInRange);

    /***
     * Freeze the engaged chunks. Should be called after engageChunks.
     * Marks chunks as freezed, prevents future updates of the engagead chunks
     * @return total number of items in the freezed range
     */
    std::shared_ptr<Rebalancer<K>> freeze();

    std::vector<Chunk<K>*>* getEngagedChunks();

    Rebalancer(Chunk<K>* chunk, KiWi<K>* chunkIterator);

    std::shared_ptr<Rebalancer<K>> compact(ScanIndex<K>* scanIndex);

    std::vector<Chunk<K>*>* getCompactedChunks();

    bool isCompacted();

    std::shared_ptr<Rebalancer<K>> shared_from_this_reb();
};


#include "Rebalancer.cpp"
#endif //KIWI_CPP_PQ_PORT_REBALANCER_HPP
