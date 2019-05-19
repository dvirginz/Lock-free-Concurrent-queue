//
// Created by dvir on 04/02/19.
//

/* questions to moshe:
 * what is the orderArray? is it the keys array? so where are the versions stored?
 * key_index - index order?
 * how come the freeze version is 1? isn't it a corner case for the start of the program?
 * The expected version of the cell should be none before the freezing?
 * prev returns null always, but used in rebalancer
 * what is noinspection
 * line 74 in scan index reset(-1) why?
 * I assumed everywhere that data!=-1
 * understand what is this <<3
 * if((c->m_sorted_count== 0 && numOfItems << 3 > g_chunk_size ) ||
 * The minus version thing is important, I've ignored it, and it will need to have special care in the future
 *
 * we need to add a sentinel chunk
 */


#ifndef KIWI_CPP_PQ_PORT_CHUNK_HPP
#define KIWI_CPP_PQ_PORT_CHUNK_HPP

#include <vector>
#include <atomic>
#include <climits>
#include <map>
#include "atomicMarkableReference.hpp"
#include "KeyCell.hpp"
#include "ThreadData.hpp"
#include "ScanIndex.hpp"
#include "config.hpp"
#include "Statistics.hpp"
#include <thread>

#define KIWI_NONE 0
#define FREEZE_VERSION 1
#define FREEZE_DEQUE_VERSION -2
#define CANCELED_REMOVE_NEXT -1

// Allocation space
#define ORDER_SIZE 4 // next + version + key + data
#define HEAD_NODE 0
#define FIRST_INDEX 1

// Constants
#define ALLOW_DUPS 1 //true
#define PAD_SIZE 100

template<typename K>
class Rebalancer;

template<typename K>
class Chunk {

//Members
public:
    std::vector<KeyCell<K>> m_keys_vec;

    //Todo determine data type (currently empty)
    std::vector<int> m_data_vec;

    std::atomic<int> m_keys_free_index;
    std::atomic_uint m_data_free_index;

    std::atomic<int> m_keys_index_serial;
    std::atomic<int> m_data_index_serial;
    std::atomic<int> m_sorted_count;

    std::vector<ThreadData> m_put_array_queue;

    Statistics<K> m_statistics;

    KeyCell<K> m_min_key; //min key value in chunk
    atomicMarkableReference<Chunk> m_next_chunk;

    //TODO this pointer should be atomic, and maybe smart?
    std::shared_ptr<Rebalancer<K>> m_rebalancer;

    Chunk* m_creator;
    //TODO does this need to be thread safe? how? why?
    std::vector<Chunk> m_children_count;

    std::atomic<bool> m_thread_frozen_this_chunk;

    // The index of the next available key (of a detached chunk).
    std::atomic<int> m_detached_next_avail_key;

    // The size of the keys (of a detached chunk) at the time of detach.
    std::atomic<int> m_detached_num_keys;

    int allocate(KeyCell<K> key, int data);

    /** base allocate method for use in allocation by implementing classes
	 * @return index of allocated order-array item (can be used to get data-array index) */
    int baseAllocate(int dataSize);

//functions
public:

    Chunk(KeyCell<K> minKey, Chunk* creator);

    Chunk(KeyCell<K> minKey);

    Chunk(Chunk<K>&& chunk){assert("no one calls it");}

    bool is_freezed();

    bool try_freeze_item(const int key_index);

    // TODO ***** COPY VALUES IS USED ONLY IN SCAN, HENCE WE WILL NOT PORT IT AS OF NOW ***

    /** this method is used by get operations (ONLY) to help pending put operations set a version
	 * @return newest item matching myKey of any currently-pending put operation */
    ThreadData help_put_in_get(int version, KeyCell<K> myKey);

    bool isRebalanced();

    /** publish data into thread array - use null to clear **/
    void publish_put(ThreadData data);

    //debugCalcCounters was not implemented
    // statistics class was not implemented
    /** gets the current version of the given order-item */
    int get_version(int keyIndex);

    /** tries to set (CAS) the version of order-item to specified version
     * @return whatever version is successfuly set (by this thread or another)	 */
    int set_version(int keyIndex, int version);
    
    int get_data(int key_index);

    bool is_infant();

    /** binary search for largest-entry smaller than 'key' in sorted part of order-array.
	 * @return the index of the entry from which to start a linear search -
	 * if key is found, its previous entry is returned! */
    int binary_find(KeyCell<K> key);

    /***
	 * Engage the chunk to a rebalancer r.
	 *
	 * @param r -- a rebalancer to engage with
	 * @return rebalancer engaged with the chunk
     */
    std::shared_ptr<Rebalancer<K>> engage(std::shared_ptr<Rebalancer<K>> r);

    /***
	 * Checks whether the chunk is engaged with a given rebalancer.
	 * @param r -- a rebalancer object. If r is null, verifies that the chunk is not engaged to any rebalancer
	 * @return true if the chunk is engaged with r, false otherwise
     */
    bool isEngaged(std::shared_ptr<Rebalancer<K>> r);

    /***
     * Fetch a rebalancer engaged with the chunk.
     * @return rebalancer object or null if not engaged.
     */
    std::shared_ptr<Rebalancer<K>> getRebalancer();

    /** marks this chunk's next pointer so this chunk is marked as deleted
	 * @return the next chunk pointed to once marked (will not change) */
    Chunk* markAndGetNext();

    /** freezes chunk so no more changes can be done in it. also marks pending items as frozen
	 * @return number of items in this chunk */
    bool freeze(int freeze_version = FREEZE_VERSION);

    /** add the given item (allocated in this chunk) to the chunk's linked list
	 * @param key_index index of item in order-array
	 * @param key given for convenience */
    void addToList(int key_index, KeyCell<K> key);

    /** finds and returns the index of the first item that is equal or larger-than the given min key
	 * with max version that is equal or less-than given version.
	 * returns KIWI_NONE if no such key exists */
    int findFirst(KeyCell<K> minKey, int version);

    /** returns the index of the first item in this chunk with a version <= version */
    int getFirst(int version);

    int findNext(int curr, int version, KeyCell<K> key);

    /** finds and returns the value for the given key, or 'null' if no such key exists */
    int find(KeyCell<K> key, ThreadData item);

    int chooseNewer(int key_idx, ThreadData threadData);

    /***
	 * Appends a new item to the end of items array. The function assumes that the array is sorted in ascending order by (Key, -Version)
	 * The method is not thread safe!!!  Should be called for  chunks accessible by single thread only.
	 *
	 * @param key_priority the key of the new item
	 * @param value the value of the new item
	 * @param version the version of the new item
     */
    void appendItem(KeyCell<K> KeyCell, int value, int version);

    int allocateSerial(KeyCell<K> key, int data);

    int baseAllocateSerial(int dataSize);

    /***
	 * Copies items from srcChunk performing compaction on the fly.
	 * @param srcChunk -- chunk to copy from
	 * @param key_index -- start position for copying
	 * @param maxCapacity -- max number of items "this" chunk can contain after copy
     * @return order index of next to the last copied item, KIWI_NONE if all items were copied
     */
    int copyPart(Chunk* srcChunk, int key_index, int maxCapacity, ScanIndex<K> scanIndex);

    //todo all original kiwi debug functions were not implemented (we will implement the needed ones)

    Chunk* newChunk(KeyCell<K> minKey);

    std::map<KeyCell<K>, ThreadData>* helpPutInScan(int myVersion, KeyCell<K> min, KeyCell<K> max);

    int copyValues(std::vector<int> *result, int idx, int myVer, KeyCell<K> min, KeyCell<K> max);
};

#include "Chunk.cpp"
#endif //KIWI_CPP_PQ_PORT_CHUNK_HPP


