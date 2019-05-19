//
// Created by dvir on 05/02/19.
//

template<typename K>
bool ItemsIterator<K>::hasNext() {
    return (m_chunk->m_keys_vec[m_current].getM_next_key_offset().load() != KIWI_NONE);
}

template<typename K>
void ItemsIterator<K>::next() {
    m_current = m_chunk->m_keys_vec[m_current].getM_next_key_offset().load();
    iter_versions->just_started = true;
}

template<typename K>
int ItemsIterator<K>::getValue() {
    return m_chunk->m_data_vec[
            m_chunk->m_keys_vec[m_current].getM_data_index().load()
    ];
}

template<typename K>
int ItemsIterator<K>::getVersion() {
    return m_chunk->m_keys_vec[m_current].getM_version().load();
}

template<typename K>
ItemsIterator<K> *ItemsIterator<K>::cloneIterator() {
    ItemsIterator* newIterator = new ItemsIterator(m_chunk);
    newIterator->m_current = this->m_current;
    return newIterator;
}

template<typename K>
VersionsIterator<K> *ItemsIterator<K>::versionsIterator() {
    return iter_versions;
}

template<typename K>
KeyCell<K> ItemsIterator<K>::getKey() {
    return m_chunk->m_keys_vec[m_current];
}

template<typename K>
ItemsIterator<K>::ItemsIterator(Chunk<K> *chunk) {
    iter_versions = new VersionsIterator<K>(HEAD_NODE,chunk);
    m_current = HEAD_NODE;
    m_chunk = chunk;
}

template<typename K>
ItemsIterator<K>::ItemsIterator(Chunk<K> *chunk, int current_index) : ItemsIterator(chunk){
    m_current = current_index;
}
