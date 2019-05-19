//
// Created by dvir on 07/02/19.
//



template<typename K>
MultiChunkIterator<K>::MultiChunkIterator(std::vector<Chunk<K> *> *chunks) {
    if(chunks == nullptr || chunks->size() == 0) throw std::invalid_argument("Iterator should have at least one item");
    first = chunks->at(0);
    last = (*chunks)[chunks->size() -1];
    current = first;
    iterCurrItem = new ItemsIterator<K>(current);
    hasNextInChunk = iterCurrItem->hasNext();
}

template<typename K>
MultiChunkIterator<K>::MultiChunkIterator(int key_index, std::vector<Chunk<K> *> *chunks) {
    if(chunks == nullptr || chunks->size() == 0) throw std::invalid_argument("Iterator should have at least one item");
    first = chunks->at(0);
    last = (*chunks)[chunks->size() -1];
    current = first;
    iterCurrItem = new ItemsIterator<K>(current,key_index);
    hasNextInChunk = iterCurrItem->hasNext();
}

template<typename K>
bool MultiChunkIterator<K>::hasNext() {
    if(iterCurrItem->hasNext()) return true;

    // cache here the information to improve next()'s performance
    hasNextInChunk = false;

    Chunk<K>* nonEmpty =  findNextNonEmptyChunk();
    if(nonEmpty == nullptr) return false;

    return true;
}

template<typename K>
void MultiChunkIterator<K>::next() {
    if(hasNextInChunk)
    {
        iterCurrItem->next();
        return;
    }

    Chunk<K>* nonEmpty = findNextNonEmptyChunk();

    current = nonEmpty;

    iterCurrItem = new ItemsIterator<K>(nonEmpty);
    iterCurrItem->next();

    hasNextInChunk = iterCurrItem->hasNext();
}

template<typename K>
KeyCell<K> MultiChunkIterator<K>::getKey() {
    return iterCurrItem->getKey();
}

template<typename K>
int MultiChunkIterator<K>::getValue() {
    return iterCurrItem->getValue();
}

template<typename K>
int MultiChunkIterator<K>::getVersion() {
    return iterCurrItem->getVersion();
}

template<typename K>
VersionsIterator<K> MultiChunkIterator<K>::versionsIterator() {
    return VersionsIterator<K>(ItemsIterator<K>(current).m_current,current);
}

template<typename K>
MultiChunkIterator<K> *MultiChunkIterator<K>::cloneIterator() {
    MultiChunkIterator* newIterator = new MultiChunkIterator();
    newIterator->first = first;
    newIterator->last = last;
    newIterator->current = current;
    newIterator->hasNextInChunk = hasNextInChunk;

    newIterator->iterCurrItem = iterCurrItem->cloneIterator();

    return  newIterator;
}

template<typename K>
Chunk<K> *MultiChunkIterator<K>::findNextNonEmptyChunk() {
    if(current == last) return nullptr;

    Chunk<K>* chunk = current->m_next_chunk.getReference();
    if(chunk == nullptr) return nullptr;

    while(chunk != nullptr)
    {
        ItemsIterator<K>* iter =  new ItemsIterator<K>(chunk);
        if(iter->hasNext()) return chunk;

        if(chunk == last) return nullptr;

        chunk = chunk->m_next_chunk.getReference();
    }

    return nullptr;
}
