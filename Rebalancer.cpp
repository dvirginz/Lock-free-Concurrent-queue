//
// Created by dvir on 06/02/19.
//

#include "Compactor.hpp"
#include "KiWi.hpp"
#include "PolicyImpl.hpp"
#include "config.hpp"
#include <memory>

template<typename K>
Rebalancer<K>::Rebalancer(Chunk<K> *chunk, KiWi<K> *chunkIterator) : m_compactedChunks(nullptr), m_engagedChunks(nullptr),
                                                            m_freezedItems(false) {
    if (chunk == NULL || chunkIterator == NULL) throw std::invalid_argument("Rebalancer construction with null args");

    m_nextToEngage = chunk;
    m_startChunk = chunk;
    m_chunkIterator = chunkIterator;
}

template<typename K>
std::vector<Chunk<K> *> *Rebalancer<K>::createEngagedList(Chunk<K> *firstChunkInRange) {
    Chunk<K> *current = firstChunkInRange;
    std::vector<Chunk<K> *> *engaged = new std::vector<Chunk<K> *>();

    while (current != NULL && current->isEngaged(shared_from_this_reb())) {
        engaged->push_back(current);
        current = current->m_next_chunk.getReference();
    }

    if (engaged->empty()) throw std::invalid_argument("Engaged list cannot be empty");

    return engaged;
}

template<typename K>
std::shared_ptr<Rebalancer<K>> Rebalancer<K>::engageChunks() {
    // the policy object will store first, last refs of engaged range
    std::unique_ptr<PolicyImpl<K>> p = std::make_unique<PolicyImpl<K>>(m_startChunk, shared_from_this_reb());

    while (true) {
        Chunk<K> *next = m_nextToEngage;
        if (next == NULL) break;

        next->engage(shared_from_this_reb());

        if (!next->isEngaged(shared_from_this_reb()) && next == m_startChunk)
            return next->getRebalancer()->engageChunks();

        // policy caches last discovered  interval [first, last] of engaged range
        // to get next candidate policy traverses from first backward,
        //  from last forward to find non-engaged chunks connected to the engaged interval
        // if we return null here the policy decided to terminate the engagement loop

        Chunk<K> *candidate = p->findNextCandidate();

        // if fail to CAS here, another thread has updated next candidate
        // continue to while loop and try to engage it
        m_nextToEngage.compare_exchange_strong(next, candidate);
    }

    p->updateRangeView();
    std::vector<Chunk<K> *> *engaged = createEngagedList(p->m_first);

    std::vector<Chunk<K> *> *null_ref_engaged = nullptr;
    if (m_engagedChunks.compare_exchange_strong(null_ref_engaged, engaged) && countCompactions) {
        config::compactionsNum.fetch_add(1);
        config::engagedChunks.fetch_add(engaged->size());
    }


    return this->shared_from_this_reb();
}

template<typename K>
std::shared_ptr<Rebalancer<K>> Rebalancer<K>::freeze() {
    if (m_freezedItems) return this->shared_from_this_reb();


    for (Chunk<K> *chunk : *m_engagedChunks.load()) {
        chunk->freeze();
    }

    m_freezedItems.store(true);

    return this->shared_from_this_reb();
}

template<typename K>
std::vector<Chunk<K> *> *Rebalancer<K>::getEngagedChunks() {
    std::vector<Chunk<K> *> *engaged = m_engagedChunks;
    if (engaged == nullptr) throw std::invalid_argument("Trying to get engaged before engagement stage completed");

    return engaged;
}

template<typename K>
std::shared_ptr<Rebalancer<K>> Rebalancer<K>::compact(ScanIndex<K> *scanIndex) {
    if (m_compactedChunks != nullptr) return this->shared_from_this_reb();

    std::unique_ptr<Compactor<K>> c = std::make_unique<Compactor<K>>();
    std::vector<Chunk<K> *> *compacted = c->compact(getEngagedChunks(), *scanIndex);

    // if fail here, another thread succeeded
    std::vector<Chunk<K> *> *null_ref = nullptr;
    m_compactedChunks.compare_exchange_strong(null_ref, compacted);

    return this->shared_from_this_reb();
}

template<typename K>
std::vector<Chunk<K> *> *Rebalancer<K>::getCompactedChunks() {
    if (!isCompacted()) throw std::invalid_argument("Trying to get compacted chunks before compaction stage completed");

    return m_compactedChunks;
}

template<typename K>
bool Rebalancer<K>::isCompacted() {
    return m_compactedChunks != nullptr;
}

template<typename K>
std::shared_ptr<Rebalancer<K>> Rebalancer<K>::shared_from_this_reb() {
    return this->shared_from_this();
}


