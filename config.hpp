//
// Created by dvir on 04/02/19.
//

#ifndef KIWI_CPP_PQ_PORT_CONFIG_HPP
#define KIWI_CPP_PQ_PORT_CONFIG_HPP

#define MAX_THREADS 12
#define REBALANCE_SIZE 2
#define countCompactions  true
#define sortedRebalanceRatio 1.8
#define MAX_ITEMS 50

#include <atomic>



class config
{
public:
    static std::atomic<int> compactionsNum;
    static std::atomic<int> engagedChunks;

private:
    // Disallow creating an instance of this object
    config() {}
};

#include "config.cpp"

#endif //KIWI_CPP_PQ_PORT_CONFIG_HPP
