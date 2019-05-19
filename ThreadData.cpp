//
// Created by dvir on 04/02/19.
//

#include <assert.h>

ThreadData::ThreadData(const int m_key_idx) : m_key_idx(m_key_idx) {
    assert(m_key_idx < 100 && "shouldn't be greater than ppa size");
}

ThreadData::ThreadData() : m_key_idx(0){

}

