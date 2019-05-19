//
// Created by dvir on 04/02/19.
//

#ifndef KIWI_CPP_PQ_PORT_KEYKeyCell_HPP
#define KIWI_CPP_PQ_PORT_KEYKeyCell_HPP


#include <string>

#include <sstream>
#include <atomic>



template<typename K=int>
class KeyCell {

public:
    std::atomic<K *> m_key;
    std::atomic<int> m_next_key_offset;
    std::atomic<int> m_data_index;
    std::atomic<int> m_version;

    typedef K value_type;

public:
    std::atomic<bool> m_is_valid;

public:
    std::atomic<int> &getM_version();

    std::atomic<int> &getM_next_key_offset();


    std::atomic<int> &getM_data_index();


    std::atomic<K *> &getM_key();


    static const KeyCell *getEmpty_KeyCell();


public:
    KeyCell(const value_type &m_key, int m_next_key_offset, int m_data_index, int m_version)
    : m_key(new K(m_key)),
      m_next_key_offset(
      m_next_key_offset),
      m_data_index(m_data_index),
      m_version(m_version),
      m_is_valid(true)
    {
    }

    KeyCell(const KeyCell& cell);
    KeyCell() : KeyCell(*empty_KeyCell) {}

private:
    static const KeyCell* empty_KeyCell;

public:

    std::string to_string();

    bool operator ==(const KeyCell& cell) const;

    bool operator <(const KeyCell& keyCell) const;

    bool operator <=(const KeyCell& keyCell) const;

    KeyCell& operator=(KeyCell other);

    bool equals(const KeyCell* other) const;

    KeyCell clone();
};

#include "KeyCell.cpp"

#endif //KIWI_CPP_PQ_PORT_KEYKeyCell_HPP
