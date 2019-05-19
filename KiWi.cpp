//
// Created by dvir on 04/02/19.
//

#include <iostream>



#ifdef HAVE_FOLLY


//std::atomic<int> g_chunk_size{MAX_ITEMS};

#if 0
template<typename K>
void foo1(std::shared_ptr<SkipListT> m_skiplist){
    typename SkipListT::Accessor accessor(m_skiplist);
    typename SkipListT::Skipper skipper(accessor);
    for (auto& elem : accessor){
        std::cout << *elem.keycell.getM_key()  << std::endl;
    }
}

template<typename K>
void foo2(std::shared_ptr<SkipListT> m_skiplist){
    typename SkipListT::Accessor accessor(m_skiplist);
    Chunk<K>* c = accessor.first()->chunk;
    while(c != nullptr){
        for (int j = 0; j < g_chunk_size; ++j) {
            if(c->m_keys_vec[j].getM_key() == 0){
                continue;
            }
            std::cout << c->m_keys_vec[j].getM_key()  << std::endl;

        }
        c = c->m_next_chunk.getReference();
    }
}
#endif

#else

#if 0
template<typename K>
void foo2(std::map<KeyCell<K>,Chunk<K>*, cmpByKeyCell<K>Only> m_skiplist){

    int prev = 0;
    bool ok = true;
    for (auto& entry : m_skiplist){
        for (int j = 0; j < g_chunk_size; ++j) {
            if(entry.second->m_keys_vec[j].getM_priority() == 0){
                continue;
            }
            std::cout << entry.second->m_keys_vec[j].getM_priority()  << std::endl;

            if(!(entry.second->m_keys_vec[j].getM_priority() - prev == 1)){
                ok = false;
//                std::cout << prev  << std::endl;
            }
            prev = entry.second->m_keys_vec[j].getM_priority();
        }

    }
    if(ok)
        std::cout << ok  << std::endl;
//    std::cout << "end rebalance"  << std::endl;

}
#endif
#endif


#ifdef HAVE_FOLLY
using namespace folly;
#endif

//template<typename K>
//extern std::map<KeyCell<K>,Chunk<K>*, cmpByKeyCellOnly<K>>* g_skiplist;

template<typename K>
KiWi<K>::KiWi(Chunk<K>* head, bool withScan) :
#ifdef HAVE_FOLLY
    m_skiplist(SkipListT::createInstance(init_head_height)),
#endif
    m_version(2)// first version is 2 - since 0 means KIWI_NONE, and -1 means FREEZE
{
//    g_skiplist = &m_skiplist;
    //todo uncomment when we have skiplist
//        skiplist = new ConcurrentSkipListMap<>();
#ifdef HAVE_FOLLY
    {
        typename SkipListT::Accessor accessor(m_skiplist);
        accessor.insert(SkipListNode<K>(head->m_min_key, head));
    }
#elif HAVE_GALOIS

    // TODO: do we need the first chunk at all? it enters infinite loop here.
//    bool res = m_skiplist->push(::SkipListNode<K>(head->m_min_key, head));
    // TODO: asserting this might be problematic because Galois
    //       already has its sentinel min and max nodes with default value (0).
//    assert(res);
#else
    m_skiplist[head->m_min_key] = head;	// add first chunk (head) into skiplist
#endif

    m_withScan = withScan;

    if (withScan) {
        //threadArray = new ThreadData[MAX_THREADS];
        m_scanArray.resize(MAX_THREADS);
    }
    else {
        //threadArray = null;
    }
}

template<typename K>
KiWi<K>::KiWi(Chunk<K>* head) : KiWi(head, true)
{
}

template<typename K>
Chunk<K> *KiWi<K>::iterateChunks(Chunk<K> *c, KeyCell<K> key) {
    // find chunk following given chunk (next)
    assert(c != nullptr && "shouldn't reach here with c as nullptr");
    Chunk<K>* next = c->m_next_chunk.getReference();

    // found chunk might be in split process, so not accurate
    // since skiplist isn't updated atomically in split/compcation, our key might belong in the next chunk
    // next chunk might itself already be split, we need to iterate the chunks until we find the correct one
    while ((next != NULL) && (next->m_min_key <= key))
    {
        c = next;
        next = c->m_next_chunk.getReference();
    }

    return c;
}

template<typename K>
int KiWi<K>::get(KeyCell<K> key) {
    Chunk<K>* c;

#ifdef HAVE_FOLLY
    typename SkipListT::Accessor accessor(m_skiplist);
    typename SkipListT::Skipper skipper(accessor);
    const bool found = skipper.to(SkipListNode<K>(key));

    if (found)
        c = skipper.data().chunk;
    else {
        // Key was not found so there are 2 options:
        // 1. We got the first chunk - return -1
        // 2. We got some other chunk or we reached the end
        //    of the chunks list - get the previous chunk.
        assert(skipper.goodPred() && "add sentinel node");
        c = skipper.predData().chunk;
    }

#else
    // find chunk matching key
    auto chunk_pair = m_skiplist.lower_bound(key);
    if(chunk_pair->first == key)
        c = chunk_pair->second;
    else if(chunk_pair != m_skiplist.begin())
        c = (--chunk_pair)->second;
    else
        //make sure val can't be -1
        return -1;
#endif

    c = iterateChunks(c,key);

    // help concurrent put operations (helpPut) set a version
    ThreadData td = c->help_put_in_get(m_version, key);

    // find item matching key inside chunk
    return c->find(key, td);
}

template<typename K>
struct DetachedChunkDescriptor
{
    bool has_chunk;
    KeyCell<K> keycell;
    Chunk<K>* chunk;
    uint thread_id;

    DetachedChunkDescriptor(){
        thread_id = (int) (std::hash<std::thread::id>()(std::this_thread::get_id()) % MAX_THREADS);
    }
};

#ifdef HAVE_FOLLY
template<typename K>
KeyCell<K> KiWi<K>::dequeue()
{
    // TODO: remove global mutex once first chunk detach locking works.
    std::lock_guard<std::recursive_mutex> lk(g_mutex);

#ifndef HAVE_FOLLY
#error TODO: add support for dequeue() with std::map.
#else

    static thread_local DetachedChunkDescriptor<K> detachedChunk;


    SkipListNode<K> *slNode = nullptr;
    KeyCell<K> empty_kc;
    empty_kc.m_is_valid = false;

//    if(detachedChunk.thread_id >= (MAX_THREADS / 5) + 1)
//        return empty_kc;
    // If we don't have a detached chunk we should detach the first one.
    if (!detachedChunk.has_chunk) {
        typename SkipListT::Accessor accessor(m_skiplist);
        slNode = const_cast<SkipListNode<K>*>(accessor.first());

        std::unique_lock<std::recursive_mutex> lck(m_create_first_chunk_mutex,std::defer_lock);
        if(!lck.try_lock())
            return empty_kc;
        // The list is empty, return an invalid node.
        if (slNode == nullptr) {
            return empty_kc;
        }

        // TODO: always detaching the first chunk creates contention,
        //       we should try something more probablistic after profiling it.

        // Otherwise, freeze the chunk and claim ownership.
        //todo make sure that a dequeue can fail
        bool is_freeze = slNode->chunk->freeze(FREEZE_DEQUE_VERSION);
        if(!is_freeze){
            return empty_kc;
        }

        // Detach chunk from skip list.
        bool res = accessor.erase(*slNode);
        if(!res){
            return empty_kc;
        }

        detachedChunk.has_chunk = true;
        detachedChunk.keycell = slNode->chunk->m_keys_vec[0];
        detachedChunk.chunk = slNode->chunk;

        std::atomic<K*>& key = detachedChunk.keycell.getM_key();
        std::atomic<int>& nextKeyOffset = detachedChunk.keycell.getM_next_key_offset();

        // TODO: detach chunk from previous chunk, needed for concurrent puts and dequeue's.
        if(*key == K() && nextKeyOffset.load() != 0) {
            detachedChunk.keycell = detachedChunk.chunk->m_keys_vec[detachedChunk.keycell.getM_next_key_offset()];
        }

    }

    // Now we have a detached chunk, extract the minimum key
    // from that chunk.
    KeyCell<K> retValue = detachedChunk.keycell;

    const int nextIdx = detachedChunk.keycell.getM_next_key_offset();

    if (nextIdx == 0) {
        detachedChunk.has_chunk = false;
    } else {
        // Update next keycell.
        detachedChunk.keycell = detachedChunk.chunk->m_keys_vec[nextIdx];
    }

    return retValue;
#endif
}

#endif // HAVE_FOLLY

#ifdef HAVE_GALOIS
template<typename K>
KeyCell<K> KiWi<K>::dequeue()
{
    // TODO: remove global mutex once first chunk detach locking works.
    std::lock_guard<std::recursive_mutex> lk(g_mutex);

    static thread_local DetachedChunkDescriptor<K> detachedChunk;


    ::SkipListNode<K> *slNode = nullptr;
    KeyCell<K> empty_kc;
    empty_kc.m_is_valid = false;

//    if(detachedChunk.thread_id >= (MAX_THREADS / 5) + 1)
//        return empty_kc;
    // If we don't have a detached chunk we should detach the first one.
    if (!detachedChunk.has_chunk) {

        std::unique_lock<std::recursive_mutex> lck(m_create_first_chunk_mutex,std::defer_lock);
        if(!lck.try_lock())
            return empty_kc;

        // The list is empty, return an invalid node.
        Galois::WorkList::SkipListNode<::SkipListNode<K>> *slNode2 = m_skiplist->peek_pop();
        if (slNode2 == nullptr) {
            return empty_kc;
        }

        // TODO: always detaching the first chunk creates contention,
        //       we should try something more probablistic after profiling it.

        // Otherwise, freeze the chunk and claim ownership.
        //todo make sure that a dequeue can fail

        slNode = &slNode2->key;
        bool is_freeze = slNode->chunk->freeze(FREEZE_DEQUE_VERSION);
        if(!is_freeze){
            return empty_kc;
        }

        // Detach chunk from skip list.
        ::SkipListNode<K> key_unused;
        bool res = m_skiplist->complete_pop(slNode2, key_unused);
        if(!res){
            // TODO: is it valid to keep chunks for "dequeue freeze"
            //       but return here? who will adopt the chunk?
            return empty_kc;
        }

        detachedChunk.has_chunk = true;
        detachedChunk.keycell = slNode->chunk->m_keys_vec[0];
        detachedChunk.chunk = slNode->chunk;

        std::atomic<K*>& key = detachedChunk.keycell.getM_key();
        std::atomic<int>& nextKeyOffset = detachedChunk.keycell.getM_next_key_offset();

        // TODO: detach chunk from previous chunk, needed for concurrent puts and dequeue's.
        if(*key == K() && nextKeyOffset.load() != 0) {
            detachedChunk.keycell = detachedChunk.chunk->m_keys_vec[detachedChunk.keycell.getM_next_key_offset()];
        }

    }

    // Now we have a detached chunk, extract the minimum key
    // from that chunk.
    KeyCell<K> retValue = detachedChunk.keycell;

    const int nextIdx = detachedChunk.keycell.getM_next_key_offset();

    if (nextIdx == 0) {
        detachedChunk.has_chunk = false;
    } else {
        // Update next keycell.
        detachedChunk.keycell = detachedChunk.chunk->m_keys_vec[nextIdx];
    }

    return retValue;
}

#endif // HAVE_GALOIS

template<typename K>
ScanIndex<K> * KiWi<K>::updateAndGetPendingScans(std::vector<Chunk<K> *> *engaged) {
    // TODO: implement versions selection by key
    KeyCell<K> minKey = engaged->at(0)->m_min_key;
    Chunk<K>* nextToRange= engaged->at(engaged->size() - 1)->m_next_chunk.getReference();
    KeyCell<K> maxKey =  nextToRange == nullptr ? *KeyCell<K>::getEmpty_KeyCell() : nextToRange->m_min_key;
    return new ScanIndex<K>(getScansArray(), minKey, maxKey);
}

template<typename K>
std::vector<ScanData<K> *> * KiWi<K>::getScansArray() {

    std::vector<ScanData<K>*>* pScans = new std::vector<ScanData<K>*>();
    bool isIncremented = false;
    int ver = -1;

    // read all pending scans
    for(int i = 0; i < MAX_THREADS; ++i)
    {
        ScanData<K>* scan = m_scanArray[i];
        if(scan != nullptr)  pScans->push_back(scan);
    }


    for(ScanData<K>* sd : *pScans)
    {
        if(sd != nullptr && sd->version == KIWI_NONE)
        {
            if(!isIncremented) {
                // increments version only once
                // if at least one pending scan has no version assigned
                ver = m_version.fetch_add(1);
                isIncremented = true;
            }
            int none_ref = KIWI_NONE;
            sd->version.compare_exchange_strong(none_ref,ver);
        }
    }

    return pScans;
}

template<typename K>
Chunk<K>* KiWi<K>::rebalance(Chunk<K> *chunk) {

    if(chunk->m_keys_free_index < 0)
        return chunk;

    std::shared_ptr<Rebalancer<K>> rebalancer = std::make_shared<Rebalancer<K>>(chunk,this);

    rebalancer = rebalancer->engageChunks();


    // freeze all the engaged range.
    // When completed, all update (put, next pointer update) operations on the engaged range
    // will be redirected to help the rebalance procedure
    rebalancer->freeze();


    std::vector<Chunk<K>*>* engaged = rebalancer->getEngagedChunks();
    // before starting compaction -- check if another thread has completed this stage
    if(!(rebalancer->isCompacted())) {
        ScanIndex<K>* index = updateAndGetPendingScans(engaged);
        rebalancer->compact(index);
    }


    // the children list may be generated by another thread

    std::vector<Chunk<K>*>* compacted = rebalancer->getCompactedChunks();



    connectToChunkList(engaged, compacted);

    updateIndex(engaged, compacted);


    return (*compacted)[0];
}

template<typename K>
bool KiWi<K>::addSentinelChunk(KeyCell<K> cell, int val) {
    std::unique_lock<std::recursive_mutex> lck(m_create_first_chunk_mutex,std::defer_lock);
    if(!lck.try_lock())
        return false;
    Chunk<K>* first_chunk = new Chunk<K>(KeyCell<K>());
#ifdef HAVE_FOLLY
    typename SkipListT::Accessor accessor(m_skiplist);
    SkipListNode<K> *slNode = const_cast<SkipListNode<K>*>(accessor.first());
   if (slNode != nullptr) {
     first_chunk->m_next_chunk.CAS(nullptr,slNode->chunk,false,false);
   }
#elif HAVE_GALOIS
    Galois::WorkList::SkipListNode<K> *slNode = m_skiplist->peek_pop();
    if (slNode != nullptr) {
        first_chunk->m_next_chunk.CAS(nullptr,slNode->key.chunk,false,false);
    }
#else
#error not supported
#endif

#ifdef HAVE_FOLLY
    accessor.insert(SkipListNode<K>(first_chunk->m_min_key, first_chunk));
#elif HAVE_GALOIS
    m_skiplist->push(::SkipListNode<K>(first_chunk->m_min_key, first_chunk));
#else
#error not supported
#endif
    put(cell,val);
    return true;
}

template<typename K>
void KiWi<K>::put(KeyCell<K> key, int val) {
    // TODO: remove global mutex once first chunk detach locking works.
    std::lock_guard<std::recursive_mutex> lk(g_mutex);

    Chunk<K>* c;

#ifdef HAVE_FOLLY
    typename SkipListT::Accessor accessor(m_skiplist);
    typename SkipListT::Skipper skipper(accessor);
    const bool found = skipper.to(SkipListNode<K>(key));

    if (found)
        c = skipper.data().chunk;
    else {
        if(!skipper.goodPred() || skipper.predData().chunk == nullptr){
            addSentinelChunk(key, val);
            return;
        }
        c = skipper.predData().chunk;
    }
#elif HAVE_GALOIS
//#error TODO: implement 
#else

    //todo delete only for debugging
    auto chunk_pair = m_skiplist.lower_bound(key);
    if(chunk_pair->first == key && chunk_pair != m_skiplist.end())
        c = chunk_pair->second;
    else if(chunk_pair != m_skiplist.begin())
        c = (--chunk_pair)->second;
    else
        c = chunk_pair->second;
#endif

    // repeat until put operation is successful
    while (true)
    {

        // the chunk we have might have been in part of split so not accurate
        // we need to iterate the chunks to find the correct chunk following it
        c = iterateChunks(c, key);


        // if chunk is infant chunk (has a parent), we can't add to it
        // we need to help finish compact for its parent first, then proceed
        // todo if we get here the rebalance(parent) is null the put fails!
        {
            Chunk<K>* parent = c->m_creator;
            if (parent != NULL && parent->m_keys_vec[1].getM_key() > 0) {
                if(parent->freeze(FREEZE_VERSION)) {
                    if (rebalance(parent) == nullptr)
                        return;
                }
            }
        }


        // allocate space in chunk for key & value
        // this also writes key&val into the allocated space
        int key_index = c->allocate(key, val);


        // if failed - chunk is full, compact it & retry
        if (key_index == -1)
        {
            c = rebalance(c);
            if (c == nullptr){
                assert(true && "shouldnt get here, probably after new freeze state");
                return;}
            continue;
        }
        if(key_index == -2)
            continue;


        if (m_withScan)
        {
            // publish put operation in thread array
            // publishing BEFORE setting the version so that other operations can see our value and help
            // this is in order to prevent us from adding an item with an older version that might be missed by others (scan/get)
            c->publish_put(ThreadData(key_index));



            if(c->is_freezed())
            {

                // if succeeded to freeze item -- it is not accessible, need to reinsert it in rebalanced chunk
                if(c->try_freeze_item(key_index)) {

                    //todo I assume public push with index -1 is invalid
                    c->publish_put(ThreadData(-1));

                    c = rebalance(c);


                    continue;
                }
            }
        }

        // try to update the version to current version, but use whatever version is successfuly set
        // reading & setting version AFTER publishing ensures that anyone who sees this put op has
        // a version which is at least the version we're setting, or is otherwise setting the version itself
        int myVersion = c->set_version(key_index, m_version);


        // if chunk is frozen, clear published data, compact it and retry
        // (when freezing, version is set to FREEZE)
        if (myVersion == FREEZE_VERSION)
        {
            // clear thread-array item if needed
            //todo I assume public push with index -1 is invalid
            c->publish_put(ThreadData(-1));

            c = rebalance(c);

            continue;
        }

        // allocation is done (and published) and has a version
        // all that is left is to insert it into the chunk's linked list
        c->addToList(key_index, key);


        // delete operation from thread array - and done
        c->publish_put(ThreadData(-1));


        if(shouldRebalance(c)){

            rebalance(c);


        }


        break;
    }
}

template<typename K>
void KiWi<K>::updateLastChild(std::vector<Chunk<K> *> *engaged, std::vector<Chunk<K> *> *children) {
    Chunk<K>* lastEngaged = engaged->at(engaged->size()-1);
    Chunk<K>* nextToLast =  lastEngaged->markAndGetNext();
    Chunk<K>* lastChild = children->at(children->size() -1);

    lastChild->m_next_chunk.CAS(nullptr,nextToLast,false,false);
}

template<typename K>
void KiWi<K>::connectToChunkList(std::vector<Chunk<K> *> *engaged, std::vector<Chunk<K> *> *children) {

    updateLastChild(engaged,children);

    Chunk<K>* firstEngaged = (*engaged)[0];

    // replace in linked list - we now need to find previous chunk to our chunk
    // and CAS its next to point to c1, which is the same c1 for all threads who reach this point.
    // since prev might be marked (in compact itself) - we need to repeat this until successful
    while (true)
    {
        // start with first chunk (i.e., head)
        Chunk<K>* prev;

#ifdef HAVE_FOLLY
        {
            typename SkipListT::Accessor accessor(m_skiplist);
            typename SkipListT::Skipper skipper(accessor);
            const bool found = skipper.to(SkipListNode<K>(firstEngaged->m_min_key));

            if (found && skipper.data() == *accessor.first())
                prev = nullptr;
            else {
                assert(skipper.goodPred() && "add sentinel node");
                prev = skipper.predData().chunk;
            }
//            bool isFirstChunk = (skipper.data() == *accessor.first());
//            SkipListNode lowerEntry;
//            lowerEntry = isFirstChunk ? skipper.data() : skipper.predData();
//            prev = (lowerEntry != *accessor.first()) ? lowerEntry.chunk : nullptr;
        }
#elif HAVE_GALOIS
        ::SkipListNode<K> elem;
        bool found = m_skiplist->floor_entry(firstEngaged->m_min_key, elem);
        prev = found ? elem.chunk : nullptr;
#else
        auto lowerEntry = (m_skiplist.lower_bound(firstEngaged->m_min_key));
        lowerEntry = (lowerEntry == m_skiplist.begin()) ? m_skiplist.begin() : (--lowerEntry);
        prev = (lowerEntry != m_skiplist.begin()) ? lowerEntry->second : nullptr;
#endif

        Chunk<K>* curr = (prev != nullptr) ? prev->m_next_chunk.getReference() : nullptr;

        // if didn't succeed to find preve through the skip list -- start from the head
        if(prev == nullptr || curr != firstEngaged) {
            prev = nullptr;

#ifdef HAVE_FOLLY
            {
                std::lock_guard<std::recursive_mutex> lck(m_create_first_chunk_mutex);
                typename SkipListT::Accessor accessor(m_skiplist);

                // TODO: the chunk list got empty so no point in relabancing.
                if (accessor.first() == nullptr || const_cast<SkipListNode<K> *>(accessor.first())->keycell.getM_key() == 0)
                    return;

                curr = accessor.first()->chunk;
            }
#elif HAVE_GALOIS
            curr = m_skiplist->get_min().chunk;
#else
            curr = m_skiplist.begin()->second;    // TODO we can store&update head for a little efficiency
#endif
            // iterate until found chunk or reached end of list
            while ((curr != firstEngaged) && (curr != nullptr)) {
                prev = curr;
                curr = curr->m_next_chunk.getReference();
            }


        }

        // chunk is head or not in list (someone else already updated list), so we're done with this part
        if ((curr == nullptr) || (prev == nullptr))
            break;

        // if prev chunk is marked - it is deleted, need to help split it and then continue
        if (prev->m_next_chunk.getMark())
        {
            rebalance(prev);
            continue;
        }

        // try to CAS prev chunk's next - from chunk (that we split) into c1
        // c1 is the old chunk's replacement, and is already connected to c2
        // c2 is already connected to old chunk's next - so all we need to do is this replacement
        auto first_engaged_ref = firstEngaged;
        if ((prev->m_next_chunk.CAS(first_engaged_ref, (*children)[0], false, false)) ||
            (!prev->m_next_chunk.getMark()))
            // if we're successful, or we failed but prev is not marked - so it means someone else was successful
            // then we're done with loop
            break;
    }

}

template<typename K>
void KiWi<K>::updateIndex(std::vector<Chunk<K> *> *engagedChunks, std::vector<Chunk<K> *> *compacted) {
    auto iterEngaged = engagedChunks->begin();
    auto iterCompacted = compacted->begin();

    Chunk<K>* firstEngaged = *iterEngaged;
    Chunk<K>* firstCompacted = *iterCompacted;

#ifdef HAVE_FOLLY
    {
        typename SkipListT::Accessor accessor(m_skiplist);
        typename SkipListT::Skipper skipper(accessor);
        bool found = skipper.to(firstEngaged->m_min_key);
        if(!found){
            accessor.insert(SkipListNode<K>(firstEngaged->m_min_key, firstCompacted));
        }else{
            (*skipper).chunk = firstCompacted;
        }
    }
#elif HAVE_GALOIS
    // Yes, searching for node looks like this, that's life *sigh*.
#define SKIPLIST_LEVELS 24

    {
        uint8_t levelmax = SKIPLIST_LEVELS;
        typedef Galois::WorkList::SkipListNode<::SkipListNode<K>> sl_node_t;

        sl_node_t *succs[levelmax] = {}, *preds[levelmax] = {};
        m_skiplist->fraser_search(firstEngaged->m_min_key, preds, succs, NULL);

        bool found = (succs[0]->key == firstEngaged->m_min_key);
        if (!found) {
            m_skiplist->push(::SkipListNode<K>(firstEngaged->m_min_key, firstCompacted));
        } else {
            succs[0]->key.chunk = firstCompacted;
        }
    }
#else
    m_skiplist[firstEngaged->m_min_key] = firstCompacted;
#endif

    // update from infant to normal
    firstCompacted->m_creator = nullptr;
    std::atomic_thread_fence(std::memory_order_acquire);

    // remove all old chunks from index.
    // compacted chunks are still accessible through the first updated chunk
    while((++iterEngaged) != engagedChunks->end())
    {
        Chunk<K>* engagedToRemove = (*iterEngaged);

#ifdef HAVE_FOLLY
    {
        typename SkipListT::Accessor accessor(m_skiplist);
        accessor.erase(engagedToRemove->m_min_key);
    }
#elif HAVE_GALOIS
        ::SkipListNode<K> out_key;
        m_skiplist->try_remove(engagedToRemove->m_min_key, out_key);
#else
        m_skiplist.erase(engagedToRemove->m_min_key);
#endif
    }

    // for simplicity -  naive lock implementation
    // can be implemented without locks using versions on next pointer in  skiplist

    while((++iterCompacted) != compacted->end()) {
        Chunk<K> *compactedToAdd = *iterCompacted;
        {
            std::lock_guard<std::mutex> lock(m_updateIndex_mutex);

#ifdef HAVE_FOLLY
        {
            typename SkipListT::Accessor accessor(m_skiplist);
            typename SkipListT::Skipper skipper(accessor);
            bool found = skipper.to(compactedToAdd->m_min_key);
            if(!found){
                accessor.insert(SkipListNode<K>(compactedToAdd->m_min_key, compactedToAdd));
            }else{
                (*skipper).chunk = compactedToAdd;
            }
        }
#elif HAVE_GALOIS
    {
        uint8_t levelmax = SKIPLIST_LEVELS;
        typedef Galois::WorkList::SkipListNode<::SkipListNode<K>> sl_node_t;

        sl_node_t *succs[levelmax] = {}, *preds[levelmax] = {};
        m_skiplist->fraser_search(firstEngaged->m_min_key, preds, succs, NULL);

        bool found = (succs[0]->key == firstEngaged->m_min_key);
        if (!found) {
            m_skiplist->push(::SkipListNode<K>(compactedToAdd->m_min_key, compactedToAdd));
        } else {
            succs[0]->key.chunk = compactedToAdd;
        }
    }

#else
            m_skiplist[compactedToAdd->m_min_key] = compactedToAdd;
#endif
            compactedToAdd->m_creator = nullptr;
        }
    }
}

template<typename K>
bool KiWi<K>::shouldRebalance(Chunk<K> *c) {

    // if another thread already runs rebalance -- skip it
    if(!c->isEngaged(nullptr)) return false;
    int numOfItems = c->m_keys_free_index;
    if(numOfItems < 0)
        return false;

    //todo understand what is this <<3
    if((c->m_sorted_count== 0 && numOfItems << 3 > g_chunk_size) ||
       (c->m_sorted_count > 0 && (c->m_sorted_count * sortedRebalanceRatio) < numOfItems) )
    {
        return true;
    }

    return false;
}

template<typename K>
void KiWi<K>::compactAllSerial() {
    Chunk<K>* c;

#ifdef HAVE_FOLLY
    {
    typename SkipListT::Accessor accessor(m_skiplist);
    c = accessor.first()->chunk;
    }
#else
    c = m_skiplist.begin()->second;
#endif

    while(c!= nullptr)
    {
        c = rebalance(c);
        c =c->m_next_chunk.getReference();
    }

#ifdef HAVE_FOLLY
    {
    typename SkipListT::Accessor accessor(m_skiplist);
    c = accessor.first()->chunk;
    }
#else
    c = m_skiplist.begin()->second;
#endif

    while(c!= nullptr)
    {
        ItemsIterator<K>* iter = new ItemsIterator<K>(c);
        KeyCell<K> prevKey = *(KeyCell<K>::getEmpty_KeyCell());
        int prevVersion = 0;

        if(iter->hasNext()) {
            iter->next();
            prevKey = iter->getKey();
            prevVersion = iter->getVersion();
        }

        while(iter->hasNext()) {
            iter->next();

            KeyCell<K> key = iter->getKey();
            int version = iter->getVersion();

            if (key < prevKey )
            {
                throw std::invalid_argument("IllegalStateException");
            }
            else if (prevKey == key)
            {
                if(prevVersion < version)
                {
                    throw std::invalid_argument("IllegalStateException");
                }
            }



        }

        c = c->m_next_chunk.getReference();
    }
    return;
}

template<typename K>
int KiWi<K>::scan(std::vector<int> *result, KeyCell<K> min, KeyCell<K> max) {
    // get current version and increment version (atomically) for this scan
    // all items beyond my version are ignored by this scan
    // the newVersion() method is used to ensure my version is published correctly,
    // so concurrent split ops will not compact items with this version (ensuring linearizability)
    int myVer = newVersion(min, max);

    Chunk<K>* c;

#ifdef HAVE_FOLLY
    {
        //todo because we don't use scan we didn't import this function
        //so it doesn't work correctly with folly, if we use it in the future
        //need to update
    typename SkipListT::Accessor accessor(m_skiplist);
    typename SkipListT::Skipper skipper(accessor);
    const bool found = skipper.to(SkipListNode<K>(min));
    const bool isFirstChunk = (skipper.good() && skipper.data() == *accessor.first());

    if (found || isFirstChunk)
        c = skipper.data().chunk;
    else if (skipper.goodPred())
        c = skipper.predData().chunk;
    else
        return -1;
    }
#else
    // find chunk matching key
    auto chunk_pair = m_skiplist.lower_bound(min);
    if(chunk_pair->first == min || chunk_pair == m_skiplist.begin())
        c = chunk_pair->second;
    else if(chunk_pair != m_skiplist.begin())
        c = (--chunk_pair)->second;
    else
        //make sure val can't be -1
        return -1;
#endif

    c = iterateChunks(c,min);

    int itemsCount = 0;
    while(true)
    {

        if(c == nullptr || max < c->m_min_key)
            break;

//             help pending put ops set a version - and get in a sorted map for use in the scan iterator
//             (so old put() op doesn't suddently set an old version this scan() needs to see,
//              but after the scan() passed it)
        std::map<KeyCell<K>, ThreadData> * items = c->helpPutInScan(myVer, min, max);

        itemsCount += c->copyValues(result, itemsCount, myVer, min, max);
        c = c->m_next_chunk.getReference();
    }

    // remove scan from scan array
    publishScan(ScanData<K>(*ScanData<K>::empty_ScanData));

    return itemsCount;
}

template<typename K>
int KiWi<K>::newVersion(KeyCell<K> min, KeyCell<K> max) {
    // create new ScanData and publish it - in it the scan's version will be stored
    ScanData<K>* sd = new ScanData<K>(min, max);
    publishScan(*sd);

    // increment global version counter and get latest
    int myVer = m_version.fetch_add(1);

    int none_ref = KIWI_NONE;
    // try to set it as this scan's version - return whatever is successfuly set
    if (sd->version.compare_exchange_strong(none_ref,myVer))
        return myVer;
    else
        return sd->version;
}

template<typename K>
void KiWi<K>::publishScan(ScanData<K> data) {
    // get index of current thread
    // since thread IDs are increasing and changing, we assume threads are created one after another (sequential IDs).
    // thus, (ThreadID % MAX_THREADS) will return a unique index for each thread in range [0, MAX_THREADS)
    int idx = (int) (std::hash<std::thread::id>()(std::this_thread::get_id()) % MAX_THREADS);

    // publish into thread array
    m_scanArray[idx] = new ScanData<K>(data);
    std::atomic_thread_fence(std::memory_order_acquire);
}
