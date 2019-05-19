//
// Created by dvir on 10/02/19.
//

#ifndef KIWI_CPP_PQ_PORT_POLICYIMPL_HPP
#define KIWI_CPP_PQ_PORT_POLICYIMPL_HPP

#include "config.hpp"
#include "Chunk.hpp"

template<typename K>
class PolicyImpl {
public:
    int RebalanceSize = REBALANCE_SIZE;

    int m_chunksInRange;
    int m_itemsInRange;

    int m_maxAfterMergeItems;


    Chunk<K>* m_first;
    Chunk<K>* m_last;
    std::shared_ptr<Rebalancer<K>> m_policy_rebalancer;


    /***
 * verifies that the chunk is not engaged and not NULL
 * @param chunk candidate chunk for range extension
 * @return true if not engaged and not NULL
 */
    bool isCandidate(Chunk<K>* chunk);

    void updateRangeFwd();
//todo prev returns NULL always, ask why
    void updateRangeBwd();


    Chunk<K>* findNextCandidate();

    void updateRangeView();

    Chunk<K>* getFirstChunkInRange();

    Chunk<K>* getLastChunkInRange();

    void addToCounters(Chunk<K>* chunk);

    PolicyImpl(Chunk<K>* startChunk, std::shared_ptr<Rebalancer<K>> policy_rebalancer);
};

#include "PolicyImpl.cpp"


#endif //KIWI_CPP_PQ_PORT_POLICYIMPL_HPP
