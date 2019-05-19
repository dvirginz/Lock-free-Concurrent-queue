//
// Created by dvir on 10/02/19.
//

#ifndef KIWI_CPP_PQ_PORT_SCANDATA_HPP
#define KIWI_CPP_PQ_PORT_SCANDATA_HPP

#include "KeyCell.hpp"

template<typename K>
class ScanData{
public:
    static const ScanData* empty_ScanData;

    ScanData(KeyCell<K> min, KeyCell<K> max) : version(0), min(min), max(max){}

    ScanData(const ScanData &scanData) : version(scanData.version.load()), min(scanData.min), max(scanData.max){}

    std::atomic<int> version;
    KeyCell<K> min;
    KeyCell<K> max;

};

#endif //KIWI_CPP_PQ_PORT_SCANDATA_HPP
