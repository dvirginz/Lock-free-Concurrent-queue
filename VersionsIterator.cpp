//
// Created by dvir on 05/02/19.
//


template<typename K>
VersionsIterator<K>::VersionsIterator(int current, Chunk<K> *chunk) : m_current(current), m_chunk(chunk){}

template<typename K>
int VersionsIterator<K>::get_value() {
    return m_chunk->m_data_vec[
            m_chunk->m_keys_vec[m_current].getM_data_index().load()
    ];
}

template<typename K>
bool VersionsIterator<K>::hasNext() {
    if(just_started) return true;
    int next = m_chunk->m_keys_vec[m_current].getM_next_key_offset().load();
    if(next == KIWI_NONE) return false;

    //TODO this compare is in the source code, understand why? why the keys need to be equal for this to work?
    return m_chunk->m_keys_vec[m_current] == m_chunk->m_keys_vec[next];
}

template<typename K>
void VersionsIterator<K>::next() {
    if(just_started)
    {
        just_started = false;
        return;
    }
    else
    {
        m_current = m_chunk->m_keys_vec[m_current].getM_next_key_offset().load();
    }
}

template<typename K>
int VersionsIterator<K>::getVersion() {
    return m_chunk->m_keys_vec[m_current].getM_version().load();
}
