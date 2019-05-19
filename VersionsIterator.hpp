//
// Created by dvir on 05/02/19.
//

#ifndef KIWI_CPP_PQ_PORT_VERSIONSITERATOR_HPP
#define KIWI_CPP_PQ_PORT_VERSIONSITERATOR_HPP


#include "Chunk.hpp"


template<typename K>
class VersionsIterator {
public:
    bool just_started = true;
    int m_current;
    Chunk<K>* m_chunk;

    VersionsIterator(int current, Chunk<K>* chunk);

    int get_value();


    int getVersion();


    bool hasNext();


    void next();
};

#include "VersionsIterator.cpp"
#endif //KIWI_CPP_PQ_PORT_VERSIONSITERATOR_HPP
