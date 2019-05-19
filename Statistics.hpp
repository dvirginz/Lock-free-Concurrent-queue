//
// Created by dvir on 10/02/19.
//

#ifndef KIWI_CPP_PQ_PORT_STATISTICS_HPP
#define KIWI_CPP_PQ_PORT_STATISTICS_HPP

#include <atomic>
#include "config.hpp"

template<typename K>
class Chunk;

template<typename K>
class Statistics {
    public:
        std::atomic<int> m_dupsCount;
        Chunk<K>* m_chunk;
        Statistics(Chunk<K>* chunk);

        /***
         *
         * @return Maximum number of items the chunk can hold
         */
        int getMaxItems();

        /***
         *
         * @return Number of items inserted into the chunk
         */
        int getFilledCount();

        /***
         *
         * @return Approximate number of items chunk may contain after compaction.
         */
        int getCompactedCount();

        void incrementDuplicates();

        int getDuplicatesCount();
};

#include "Statistics.cpp"

#endif //KIWI_CPP_PQ_PORT_STATISTICS_HPP
