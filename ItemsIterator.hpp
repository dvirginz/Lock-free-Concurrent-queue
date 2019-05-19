//
// Created by dvir on 05/02/19.
//

#ifndef KIWI_CPP_PQ_PORT_ITEMSITERATOR_HPP
#define KIWI_CPP_PQ_PORT_ITEMSITERATOR_HPP


#include "VersionsIterator.hpp"


template<typename K>
class ItemsIterator {
public:
        int m_current;
        Chunk<K>* m_chunk;
        VersionsIterator<K>* iter_versions;
public:
    explicit ItemsIterator(Chunk<K>* chunk);
    ItemsIterator(Chunk<K>* chunk, int current_index);

        bool hasNext();

        void next();

        KeyCell<K> getKey();

        int getValue();

        int getVersion();

        ItemsIterator<K>* cloneIterator();

        VersionsIterator<K>* versionsIterator();

};

#include "ItemsIterator.cpp"


#endif //KIWI_CPP_PQ_PORT_ITEMSITERATOR_HPP
