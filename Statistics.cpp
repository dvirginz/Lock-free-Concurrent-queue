//
// Created by dvir on 10/02/19.
//

template<typename K>
Statistics<K>::Statistics(Chunk<K> *chunk) :m_chunk(chunk){}

extern std::atomic<int> g_chunk_size;

template<typename K>
int Statistics<K>::getMaxItems() {
    return g_chunk_size;
}

template<typename K>
int Statistics<K>::getFilledCount() {
    return m_chunk->m_keys_free_index;
}

template<typename K>
int Statistics<K>::getCompactedCount() {
    return getFilledCount() - getDuplicatesCount();
}

template<typename K>
void Statistics<K>::incrementDuplicates() {
    m_dupsCount.fetch_add(1);
}

template<typename K>
int Statistics<K>::getDuplicatesCount() {
    return m_dupsCount;
}
