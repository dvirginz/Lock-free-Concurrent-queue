//
// Created by dvir on 05/02/19.
//

#ifndef KIWI_CPP_PQ_PORT_SCANINDEX_HPP
#define KIWI_CPP_PQ_PORT_SCANINDEX_HPP

#include <vector>
#include "ThreadData.hpp"
#include "ScanData.hpp"
#include <algorithm>

template<typename K>
class ScanIndex {

public:

    std::vector<int> m_scanVersions;
    std::vector<KeyCell<K>> m_fromKeys;
    std::vector<KeyCell<K>> m_toKeys;
    bool isFirst;
    bool isOutOfRange;
    int index;
    int m_numOfVersions;
    KeyCell<K> min_cell;
    KeyCell<K> max_cell;
    KeyCell<K> currKey;

    ScanIndex(std::vector<ScanData<K> *> *scans, KeyCell<K> &minKey, KeyCell<K> &maxKey);

    void reset(KeyCell<K> key);

        /***
         *
         *
         * @param version -- we assume that version > 0
         * @return
         */
    bool shouldKeep(int version);

    void savedVersion();

};
#include "ScanIndex.cpp"


#endif //KIWI_CPP_PQ_PORT_SCANINDEX_HPP
