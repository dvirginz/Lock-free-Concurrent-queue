//
// Created by dvir on 10/02/19.
//

#include "Rebalancer.hpp"
#include <cmath>

template<typename K>
Chunk<K> *PolicyImpl<K>::findNextCandidate() {

    updateRangeView();

    // allow up to RebalanceSize chunks to be engaged
    if(m_chunksInRange >= RebalanceSize) return NULL;

    Chunk<K>* next = m_last->m_next_chunk.getReference();
    Chunk<K>* prev = NULL;
    Chunk<K>* candidate = NULL;

    if(!isCandidate(next)) next = NULL;
    if(!isCandidate(prev)) prev = NULL;

    if(next == NULL && prev == NULL) return NULL;

    if(next == NULL) {
        candidate = prev;
    }
    else if(prev == NULL)
    {
        candidate = next;
    }
    else {
        candidate = prev->m_statistics.getCompactedCount() < next->m_statistics.getCompactedCount() ? prev : next;
    }


    int newItems = candidate->m_statistics.getCompactedCount();
    int totalItems = m_itemsInRange + newItems;


    int chunksAfterMerge = (int)std::ceil(((double)totalItems)/m_maxAfterMergeItems);

    // if the the chosen chunk may reduce the number of chunks -- return it as candidate
    if( chunksAfterMerge < m_chunksInRange + 1) {
        return candidate;
    } else
    {
        return NULL;
    }
}

template<typename K>
void PolicyImpl<K>::updateRangeView() {

    updateRangeFwd();
    updateRangeBwd();

}

template<typename K>
Chunk<K> *PolicyImpl<K>::getFirstChunkInRange() {
    return m_first;
}

template<typename K>
Chunk<K> *PolicyImpl<K>::getLastChunkInRange() {
    return m_last;
}

template<typename K>
void PolicyImpl<K>::addToCounters(Chunk<K> *chunk) {
    m_itemsInRange += chunk->m_statistics.getCompactedCount();
    m_chunksInRange++;
}

template<typename K>
void PolicyImpl<K>::updateRangeBwd() {
    while(true)
    {
        Chunk<K>* prev = NULL;
        if(prev == NULL || !prev->isEngaged(m_policy_rebalancer)) break;
        // double check here, after we know that prev is engaged, thus cannot be updated
        if(prev->m_next_chunk.getReference() == m_first) {
            m_first = prev;
            addToCounters(m_first);
        }
    }
}

template<typename K>
void PolicyImpl<K>::updateRangeFwd() {

    while(true) {
        Chunk<K>* next = m_last->m_next_chunk.getReference();
        if (next == NULL || !next->isEngaged(m_policy_rebalancer)) break;
        m_last = next;
        addToCounters(m_last);
    }
}

template<typename K>
bool PolicyImpl<K>::isCandidate(Chunk<K> *chunk) {
    // do not take chunks that are engaged with another rebalancer or infant
    if(chunk == NULL || !chunk->isEngaged(m_policy_rebalancer) || chunk->is_infant()) return false;
    return true;
}

template<typename K>
PolicyImpl<K>::PolicyImpl(Chunk<K> *startChunk, std::shared_ptr<Rebalancer<K>> policy_rebalancer) {
    if(startChunk == NULL) throw std::invalid_argument("startChunk is NULL in policy");
    m_first = startChunk;
    m_last = startChunk;
    m_chunksInRange = 1;
    m_itemsInRange = startChunk->m_statistics.getCompactedCount();
    m_maxAfterMergeItems = (int)(g_chunk_size * MAX_AFTER_MERGE_PART);
    m_policy_rebalancer = policy_rebalancer;
}
