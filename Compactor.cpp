//
// Created by dvir on 07/02/19.
//


template<typename K>
std::vector<Chunk<K> *> *Compactor<K>::compact(std::vector<Chunk<K> *> *frozenChunks, ScanIndex<K> scanIndex) {
    auto firstFrozen = frozenChunks->begin();
    auto currFrozen = firstFrozen;
    Chunk<K>* currCompacted = (*firstFrozen)->newChunk((*firstFrozen)->m_min_key);

    int key_index = (*firstFrozen)->m_keys_vec[HEAD_NODE].getM_next_key_offset();

    std::vector<Chunk<K>*>* compacted = new std::vector<Chunk<K>*>();

    while(true)
    {
        key_index = currCompacted->copyPart((*currFrozen), key_index, LOW_THRESHOLD(), scanIndex);

        // if completed reading curr freezed chunk
        if(key_index == KIWI_NONE)
        {
            if(!(currFrozen++ == frozenChunks->end()))
                break;

            currFrozen++;
            key_index = (*currFrozen)->m_keys_vec[HEAD_NODE].getM_next_key_offset();
        }
        else // filled compacted chunk up to LOW_THRESHOLD
        {
            std::vector<Chunk<K>*> frozenSuffix(currFrozen,frozenChunks->end());

            // try to look ahead and add frozen suffix
            if(canAppendSuffix(key_index, &(frozenSuffix), MAX_RANGE_TO_APPEND()))
            {
                completeCopy(currCompacted, key_index, &(frozenSuffix), scanIndex);
                break;

            } else
            {
                Chunk<K>* c = (*firstFrozen)->newChunk((*currFrozen)->m_keys_vec[key_index]);
                currCompacted->m_next_chunk.set(c,false);

                compacted->push_back(currCompacted);
                currCompacted = c;
            }
        }

    }

    compacted->push_back(currCompacted);

    return compacted;
}

template<typename K>
bool Compactor<K>::canAppendSuffix(int key_index, std::vector<Chunk<K> *> *frozenSuffix, int maxCount) {
    MultiChunkIterator<K> iter = MultiChunkIterator<K>(key_index, frozenSuffix);
    int counter = 1;

    while(iter.hasNext() && counter < maxCount)
    {
        iter.next();
        counter++;
    }

    return counter < maxCount;
}

template<typename K>
void Compactor<K>::completeCopy(Chunk<K> *dest, int key_index, std::vector<Chunk<K> *> *srcChunks, ScanIndex<K> scanIndex) {
    auto src = srcChunks->begin();
    dest->copyPart(*src,key_index, g_chunk_size, scanIndex);

    while(src + 1 != srcChunks->end())
    {

        src++;
        key_index = (*src)->m_keys_vec[HEAD_NODE].getM_next_key_offset();
        dest->copyPart(*src,key_index, g_chunk_size, scanIndex);
    }
}
