//
// Created by dvir on 04/02/19.
//

#ifndef KIWI_CPP_PQ_PORT_KIWI_HPP
#define KIWI_CPP_PQ_PORT_KIWI_HPP


#include <vector>
#include <atomic>
#include <map>
#include <thread>
#include <mutex>
#include "ThreadData.hpp"
#include "ScanData.hpp"
#include "Rebalancer.hpp"
#include "ItemsIterator.hpp"
#include "Chunk.hpp"
#include "SkipListNodeDecl.h"

#ifdef HAVE_FOLLY
#include "folly/ConcurrentSkipList.h"
#include <memory>

using namespace folly;
#elif HAVE_GALOIS
#include "Galois/config.h"
#include "Galois/Runtime/mm/Mem.h"
#include "Galois/optional.h"
#include "Galois/Threads.h"
#include "Galois/FlatMap.h"
#include "Galois/Timer.h"
#include "Galois/Runtime/PerThreadStorage.h"
#include "Galois/WorkList/Fifo.h"
#include "Galois/WorkList/WorkListHelpers.h"

using namespace Galois::WorkList;
#endif

extern std::atomic<int> g_chunk_size;

template<typename K>
class KiWi {
    std::recursive_mutex g_mutex;

    /*************** Members ***************/
public:
    //todo make it skiplist
#ifdef HAVE_FOLLY
    using SkipListT = ConcurrentSkipList<SkipListNode<K>, SkipListNodeComparator<K>>;
    static const int init_head_height = 1;
    //typedef folly::ConcurrentSkipList<SkipListNode<K>, SkipListNodeComparator<K>> SkipListT;
    // TODO: shared pointers on multi threaded application make
    //       contention, see if we could avoid it as the skiplist
    //       itself is already concurrent.
    std::shared_ptr<SkipListT> m_skiplist;              // skiplist of chunks for fast navigation
#elif HAVE_GALOIS
    // TODO: verify that it is greater than.
    //using SkipListT = LockFreeSkipList<SkipListNodeComparator<K>, SkipListNode<K>>;
    using SkipListT = LockFreeSkipList<SkipListNodeComparator<K>, ::SkipListNode<K>>;
    std::shared_ptr<SkipListT> m_skiplist;
#else
    std::map<KeyCell,Chunk<K> *, cmpByKeyCellOnly> m_skiplist;		// skiplist of chunks for fast navigation
#endif
    std::atomic<int> m_version;		// current version to add items with
    bool m_withScan;		// support scan operations or not (scans add thread-array)
    std::vector<ScanData<K>*> m_scanArray;
    std::mutex m_updateIndex_mutex;  // protects updateIndex
    std::recursive_mutex m_create_first_chunk_mutex;  // protects dequeue



public:
    KiWi(Chunk<K>* head, bool withScan);
    
    KiWi(Chunk<K>* head);

    /** finds and returns the chunk where key should be located, starting from given chunk */
    Chunk<K> * iterateChunks(Chunk<K>* c, KeyCell<K> key);

    int get(KeyCell<K> key);

    bool addSentinelChunk(KeyCell<K> cell, int val);

    void put(KeyCell<K> key, int val);

    KeyCell<K> dequeue();

    Chunk<K> * rebalance(Chunk<K> *chunk);

    ScanIndex<K> *updateAndGetPendingScans(std::vector<Chunk<K> *> *engaged);

    std::vector<ScanData<K> *> *getScansArray();

    void connectToChunkList(std::vector<Chunk<K>*>* engaged, std::vector<Chunk<K>*>* children);

    void updateLastChild(std::vector<Chunk<K>*>* engaged, std::vector<Chunk<K>*>* children);

    void updateIndex(std::vector<Chunk<K>*>* engagedChunks, std::vector<Chunk<K>*>* compacted);

    bool shouldRebalance(Chunk<K>* c);

    void compactAllSerial();

    /** publish data into thread array - use null to clear **/
    void publishScan(ScanData<K> data);

    /** fetch-and-add for the version counter. in a separate method because scan() ops need to use
	 * thread-array for this, to make sure concurrent split/compaction ops are aware of the scan() */
    int newVersion(KeyCell<K> min, KeyCell<K> max);

    int scan(std::vector<int>* result, KeyCell<K> min, KeyCell<K> max);


};

#include "KiWi.cpp"

#endif //KIWI_CPP_PQ_PORT_KIWI_HPP
