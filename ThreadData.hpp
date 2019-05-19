//
// Created by dvir on 04/02/19.
//

#ifndef KIWI_CPP_PQ_PORT_THREADDATA_HPP
#define KIWI_CPP_PQ_PORT_THREADDATA_HPP


#include <atomic>

class ThreadData {
public:
    int m_key_idx;

    ThreadData(const int m_key_idx);
    ThreadData();
};

#include "ThreadData.cpp"


#endif //KIWI_CPP_PQ_PORT_THREADDATA_HPP
