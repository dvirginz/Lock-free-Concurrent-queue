//
// Created by dvir on 21/02/19.
//

#ifndef KIWI_CPP_PQ_PORT_SKIPLISTNODEDECL_H
#define KIWI_CPP_PQ_PORT_SKIPLISTNODEDECL_H

#include "KeyCell.hpp"

// TODO: move to a separate header.

#if 0
template<typename K>
class KeyCell;
#endif

template<typename K>
class Chunk;

template<typename K>
class SkipListNode
{
public:
    // TODO: do we still need it?
    SkipListNode<K>() = default;
    SkipListNode<K>(KeyCell<K> keycell_, Chunk<K>* chunk_=nullptr)
            : keycell(keycell_), chunk(chunk_)
    {
    }

    bool operator==(const SkipListNode<K>& rhs) const
    {
        const KeyCell<K>& klhs = this->keycell;
        const KeyCell<K>& krhs = rhs.keycell;
        return (klhs.m_key == krhs.m_key);
    }

    bool operator!=(const SkipListNode<K>& rhs) const
    {
        return !(*this == rhs);
    }

    bool operator<(const SkipListNode<K>& rhs) const
    {
        const KeyCell<K>& klhs = this->keycell;
        const KeyCell<K>& krhs = rhs.keycell;
        return (klhs.m_key < krhs.m_key);
    }

    KeyCell<K> keycell;
    Chunk<K>* chunk;
};

template<typename K>
class SkipListNodeComparator
{
public:
    bool operator()(const SkipListNode<K>& lhs, const SkipListNode<K>& rhs) const
    {
        const KeyCell<K>& klhs = lhs.keycell;
        const KeyCell<K>& krhs = rhs.keycell;
        return ((*(klhs.m_key) < *(krhs.m_key)));
    }
};

#endif //KIWI_CPP_PQ_PORT_SKIPLISTNODEDECL_H
