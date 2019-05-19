//
// Created by dvir on 04/02/19.
//

#include "Rebalancer.hpp"
#include <algorithm>    // std::fill

#include "KeyCell.hpp"

template<typename K>
struct cmpByKeyCellOnly {
    bool operator()(const KeyCell<K>& a, const KeyCell<K>& b) const {
        return a < b;
    }
};

template<typename K>
std::map<KeyCell<K>,Chunk<K> *, cmpByKeyCellOnly<K>>* g_skiplist;		// skiplist of Chunk<K> *s for fast navigation

template<typename K>
void foo2(std::map<KeyCell<K>,Chunk<K> *, cmpByKeyCellOnly<K>> m_skiplist);

template<typename K>
Chunk<K>::Chunk(KeyCell<K> minKey, Chunk<K> * creator) : Chunk<K>(minKey)
{
    this->m_creator = creator;
}

template<typename K>
Chunk<K>::Chunk(KeyCell<K> minKey) :
                               m_keys_vec(g_chunk_size),
                               m_data_vec(g_chunk_size), m_keys_free_index(FIRST_INDEX),
                               m_data_free_index(FIRST_INDEX), m_keys_index_serial(FIRST_INDEX),
                               m_data_index_serial(FIRST_INDEX),m_sorted_count(0),
                               m_put_array_queue(MAX_THREADS, 0),m_statistics(this), m_min_key(minKey),
                               m_rebalancer(nullptr), m_creator(nullptr),
                               m_thread_frozen_this_chunk(false) {
    std::fill(this->m_put_array_queue.begin(), this->m_put_array_queue.begin() + MAX_THREADS, ThreadData(0));
    std::fill(this->m_keys_vec.begin(), this->m_keys_vec.begin() + g_chunk_size, KeyCell<K>(K(), 0, 0, 0));
    std::fill(this->m_data_vec.begin(), this->m_data_vec.begin() + g_chunk_size, 0);
}

template<typename K>
bool Chunk<K>::is_freezed() {
    return m_keys_free_index < 0 || (uint) m_keys_free_index.load() >= m_keys_vec.size();
}

template<typename K>
bool Chunk<K>::try_freeze_item(const int key_index) {
    int valid_version_before_swap = KIWI_NONE;
    return m_keys_vec[key_index].getM_version().compare_exchange_strong(valid_version_before_swap, FREEZE_VERSION);
}

template<typename K>
int Chunk<K>::set_version(int keyIndex, int version) {
    int previous_version = KIWI_NONE;
    //TODO the version in -1, understand why
    if (m_keys_vec[keyIndex].getM_version().compare_exchange_strong(previous_version, -version))
        return version;

        // if failed (someone else's CAS succeeded) - read version again and return it
    else
        return m_keys_vec[keyIndex].getM_version();
}

template<typename K>
int Chunk<K>::get_version(int keyIndex) {
    return abs(m_keys_vec[keyIndex].getM_version());
}

template<typename K>
bool Chunk<K>::isRebalanced() {
    return std::atomic_load(&m_rebalancer) == nullptr ? false : std::atomic_load(&m_rebalancer)->isCompacted();
}

template<typename K>
ThreadData Chunk<K>::help_put_in_get(int version, KeyCell<K> myKey) {
    // marks the most recent put that was found in the thread-array
    ThreadData newest_put;
    int newest_ver = KIWI_NONE;

    // go over thread data of all threads
    for (int i = 0; i < MAX_THREADS; ++i) {
        // make sure data is for a Put operation
        ThreadData curr_put = m_put_array_queue[i];
        if (curr_put.m_key_idx == 0)
            continue;

        // if put operation's key is not same as my key - skip it
        KeyCell<K> currKey = m_keys_vec[curr_put.m_key_idx];
        if (!(currKey == myKey))
            continue;

        // read the current version of the item
        int currVer = currKey.getM_version().load();

        // if empty, try to set to my version
        if (currVer == KIWI_NONE)
            currVer = set_version(curr_put.m_key_idx, version);

        // if item is frozen - skip it
        if (currVer == FREEZE_VERSION)
            continue;

        // current item has newer version than newest item - replace
        if (currVer > newest_ver) {
            newest_ver = currVer;
            newest_put = curr_put;
        }
            // same version for both item - check according to Chunk<K> *
        else if (currVer == newest_ver) {
            // same Chunk<K> *& version but current's index is larger - it is newer
            if (curr_put.m_key_idx > newest_put.m_key_idx)
                newest_put = curr_put;
        }
    }

    // return item if its Chunk<K> *.child1 is null, otherwise return null
    if ((newest_put.m_key_idx == 0) || (isRebalanced()))
        return ThreadData(0);
    else
        return newest_put;
}

template<typename K>
void Chunk<K>::publish_put(ThreadData data) {
    // get index of current thread
    // since thread IDs are increasing and changing, we assume threads are created one after another (sequential IDs).
    // thus, (ThreadID % MAX_THREADS) will return a unique index for each thread in range [0, MAX_THREADS)
    int idx = (int) (std::hash<std::thread::id>()(std::this_thread::get_id()) % MAX_THREADS);

    // publish into thread array
    m_put_array_queue[idx] = data;
    std::atomic_thread_fence(std::memory_order_seq_cst);
}


template<typename K>
int Chunk<K>::get_data(int key_index) {

    //TODO understand what is the data type of the data, because now -1 is returned as null;
    int data_idx = this->m_keys_vec[key_index].getM_data_index().load();
    return (data_idx >= 0) ? this->m_data_vec[data_idx] : -1;
}


template<typename K>
bool Chunk<K>::is_infant() {
    return m_creator != nullptr;
}


template<typename K>
int Chunk<K>::binary_find(KeyCell<K> key) {
    // if there are no sorted keys,or the first item is already larger than key -
    // return the head node for a regular linear search
    if ((m_sorted_count == 0) || key <= m_keys_vec[FIRST_INDEX])
        return HEAD_NODE;

    int start = 0;
    int end = m_sorted_count;

    while (end - start > 1) {
        int curr = start + (end - start) / 2;

        if (key <= m_keys_vec[curr])
            end = curr;
        else
            start = curr;
    }

    return start;
}

//todo why r need to be atomic reference also? (in original code)

template<typename K>
std::shared_ptr<Rebalancer<K>> Chunk<K>::engage(std::shared_ptr<Rebalancer<K>> r) {
    std::shared_ptr<Rebalancer<K>> before_engage_rebalancer(nullptr);
    std::atomic_compare_exchange_strong(&m_rebalancer,&before_engage_rebalancer, r);
    return std::atomic_load(&m_rebalancer);
}


template<typename K>
bool Chunk<K>::isEngaged(std::shared_ptr<Rebalancer<K>> r) {
    return (std::atomic_load(&m_rebalancer) == r);
}


template<typename K>
std::shared_ptr<Rebalancer<K>> Chunk<K>::getRebalancer() {
    return std::atomic_load(&m_rebalancer);
}


template<typename K>
Chunk<K> * Chunk<K>::markAndGetNext() {
    // new Chunk<K> *s are ready, we mark frozen Chunk<K> *'s next pointer so it won't change
    // since next pointer can be changed by other split operations we need to do this in a loop - until we succeed
    while (true) {
        // if Chunk<K> *is marked - that is ok and its next pointer will not be changed anymore
        // return whatever Chunk<K> *is set as next
        if (m_next_chunk.getMark()) {
            return m_next_chunk.getReference();
        }
            // otherwise try to mark it
        else {
            // read Chunk<K> *'s current next
            Chunk<K> *savedNext = m_next_chunk.getReference();

            // try to mark next while keeping the same next Chunk<K> *- using CAS
            // if we succeeded then the next pointer we remembered is set and will not change - return it
            if (m_next_chunk.CAS(savedNext, savedNext, false, true))
                return savedNext;
        }
    }
}


template<typename K>
void Chunk<K>::addToList(int key_index, KeyCell<K> key) {
    int prev, curr;
    int ancor = -1;

    // retry adding to list until successful
    // no items are removed from list - so we don't need to restart on failures
    // so if we CAS some node's next and fail, we can continue from it
    // --retry so long as version is negative (which means item isn't in linked-list)

    //TODO understand how and where the version can be less than zero? maybe my initialization is wrong?
    while (m_keys_vec[key_index].getM_version() < 0) {
        // remember next pointer in entry we're trying to add
        int savedNext = m_keys_vec[key_index].getM_next_key_offset();

        // start iterating from quickly-found node (by binary search) in sorted part of order-array
        if (ancor == -1) ancor = binary_find(key);
        curr = ancor;

        // iterate items until key's position is found
        while (true) {
            prev = curr;
            curr = m_keys_vec[prev].getM_next_key_offset();    // index of next item in list

            // if no item, done searching - add to end of list
            if (curr == KIWI_NONE)
                break;

            // if found item we're trying to insert - already inserted by someone else, so we're done
            if (curr == key_index)
                return;
            //TODO also update version to positive? todo in the official code, understand why


            // if current item's key is larger, done searching - add between prev and curr
            if (key < m_keys_vec[curr])
                break;

            // if same key - check according to version and location in array
            if (key == m_keys_vec[curr]) {
                // if duplicate values aren't allowed - do not add value
                if (!ALLOW_DUPS) {
                    return;
                }

                int ver_mine = m_keys_vec[key_index].getM_version();
                int ver_next = m_keys_vec[curr].getM_version();

                // if current item's version is smaller, done searching - larger versions are first in list
                if (ver_next < ver_mine)
                    break;

                // same versions but i'm later in Chunk<K> *'s array - i'm first in list
                if (ver_next == ver_mine) {
                    int newDataIdx = m_keys_vec[key_index].getM_data_index();
                    int oldDataIdx = m_keys_vec[curr].getM_data_index();

                    while ((std::abs(newDataIdx) > std::abs(oldDataIdx)) &&
                           !(m_keys_vec[curr].getM_data_index().compare_exchange_strong(oldDataIdx, newDataIdx))) {
                        oldDataIdx = m_keys_vec[curr].getM_data_index();
                    }

                    return;
                }
            }
        }

        if (savedNext == CANCELED_REMOVE_NEXT) return;
        if (!(m_keys_vec[curr] == key) && savedNext == KIWI_NONE && m_keys_vec[key_index].getM_data_index() < 0) {
            if (m_keys_vec[key_index].getM_next_key_offset().compare_exchange_strong(savedNext, CANCELED_REMOVE_NEXT))
                return;
            else
                continue;
        }

        // try to CAS update my next to current item ("curr" variable)
        // using CAS from saved next since someone else might help us
        // and we need to avoid race conditions with other put ops and helpers
        int savedNextRef = savedNext;
        if (m_keys_vec[key_index].getM_next_key_offset().compare_exchange_strong(savedNextRef, curr)) {
            // try to CAS curr's next to point from "next" to me
            // if successful - we're done, exit loop. Otherwise retry (return to "while true" loop)
            int currRef = curr;
            if (m_keys_vec[prev].getM_next_key_offset().compare_exchange_strong(currRef, key_index)) {
                // if some CAS failed we restart, if both successful - we're done
                // update version to positive (getVersion() always returns positive number) to mark item is linked

                m_keys_vec[key_index].getM_version().store(std::abs(m_keys_vec[key_index].getM_version()));

                // if adding version for existing key -- update duplicates statistics
                if (m_keys_vec[curr] == key) {
                    m_statistics.incrementDuplicates();
                }
                break;
            }
        }
    }
}


template<typename K>
bool Chunk<K>::freeze(int freeze_version) {

    m_detached_num_keys = m_keys_free_index.load();

    // TODO: is it always 0?
    m_detached_next_avail_key = 0;

    // prevent new puts to the Chunk<K> *
    int free_index_before_mark;
    if(freeze_version == FREEZE_DEQUE_VERSION){
        free_index_before_mark = m_keys_free_index.exchange(-m_keys_vec.size());
        if(free_index_before_mark > 0 && (uint)free_index_before_mark > m_keys_vec.size()){
            m_keys_free_index.store(m_keys_vec.size() + 1);
            return false;
        }
    }
    else{
        free_index_before_mark = m_keys_free_index.fetch_add(m_keys_vec.size());
        if(free_index_before_mark < 0){
            m_keys_free_index.store(-m_keys_vec.size());
            return false;
        }
    }

    // go over thread data of all threads
    for (int i = 0; i < MAX_THREADS; ++i) {
        // make sure data is for a Put operatio
        ThreadData curr_put = m_put_array_queue[i];

        if (curr_put.m_key_idx == 0 || curr_put.m_key_idx == -1)
            continue;

        int idx = curr_put.m_key_idx;
        int version = m_keys_vec[idx].getM_version();

        // if item is frozen, ignore it - so only handle non-frozen items
        if (version != freeze_version) {
            // if item has no version, try to freeze it
            if (version == KIWI_NONE) {
                // set version to FREEZE so put op knows to restart
                // if succeded - item will not be in this Chunk<K> *, we can continue to next item
                int previous_version = KIWI_NONE;
                if ((m_keys_vec[idx].getM_version().compare_exchange_strong(previous_version, freeze_version)) ||
                    m_keys_vec[idx].getM_version().load() == freeze_version) {
                    continue;
                }
            }

            // if we reached here then item has a version - we need to help by adding item to Chunk<K> *'s list
            // we need to help the pending put operation add itself to the list before proceeding
            // to make sure a frozen Chunk<K> *is actually frozen - all items are fully added
            addToList(idx, m_keys_vec[idx]);

        }
    }
    return true;
}


template<typename K>
int Chunk<K>::findFirst(KeyCell<K> minKey, int version) {
    // binary search sorted part of order-array to quickly find node to start search at
    // it finds previous-to-key so start with its next
    int curr = m_keys_vec[binary_find(minKey)].getM_next_key_offset();

    // iterate until end of list (or key is found)
    while (curr != KIWI_NONE) {
        KeyCell<K> key = m_keys_vec[curr];

        // if item's key is larger or equal than min - we've found a matching key
        if (minKey <= key) {
            // check for valid version
            if (m_keys_vec[curr].getM_version() <= version) {
                return curr;
            }
        }

        curr = m_keys_vec[curr].getM_next_key_offset();
    }

    return KIWI_NONE;
}


template<typename K>
int Chunk<K>::getFirst(int version) {
    int curr = m_keys_vec[0].getM_next_key_offset();

    // iterate over all items
    while (curr != KIWI_NONE) {
        // if current item is of matching version - return it
        if (m_keys_vec[curr].getM_version() <= version) {
            return curr;
        }

        // proceed to next item
        curr = m_keys_vec[curr].getM_next_key_offset();
    }
    return KIWI_NONE;
}


template<typename K>
int Chunk<K>::findNext(int curr, int version, KeyCell<K> key) {
    curr = m_keys_vec[curr].getM_next_key_offset();

    while (curr != KIWI_NONE) {
        KeyCell<K> currKey = m_keys_vec[curr];

        // if in a valid version, and a different key - found next item
        if (!(currKey == key) && (m_keys_vec[curr].getM_version() <= version)) {
            return curr;
        }

        // otherwise proceed to next
        curr = m_keys_vec[curr].getM_next_key_offset();
    }

    return KIWI_NONE;
}


template<typename K>
int Chunk<K>::find(KeyCell<K> key, ThreadData item) {
    // binary search sorted part of order-array to quickly find node to start search at
    // it finds previous-to-key so start with its next
    int curr = m_keys_vec[binary_find(key)].getM_next_key_offset();

    // iterate until end of list (or key is found)
    while (curr != KIWI_NONE) {
        // if item's key is larger - we've exceeded our key
        // it's not in Chunk<K> *- no need to search further
        if (key < m_keys_vec[curr])
            //todo make sure that find with -1 as value is "not found"
            return -1;
            // if keys are equal - we've found the item
        else if (m_keys_vec[curr] == key)
            return chooseNewer(curr, item);
            // otherwise- proceed to next item
        else
            curr = m_keys_vec[curr].getM_next_key_offset();
    }

    return 0;
}


template<typename K>
int Chunk<K>::chooseNewer(int key_idx, ThreadData threadData) {
    // if threadData is empty or in different Chunk<K> *, then key_idx is definitely newer
    // it's true since put() publishes after finding a Chunk<K> *, and get() finds Chunk<K> *only after reading thread-array
    // so get() definitely sees the same Chunk<K> *s put() sees, or NEWER Chunk<K> *s
    if ((threadData.m_key_idx = 0))
        return m_data_vec[m_keys_vec[key_idx].getM_data_index().load()];

    // if same Chunk<K> *then regular comparison (version, then orderIndex)
    int itemVer = m_keys_vec[key_idx].getM_version();
    int dataVer = m_keys_vec[threadData.m_key_idx].getM_version();

    if (itemVer > dataVer)
        return m_data_vec[m_keys_vec[key_idx].getM_data_index().load()];
    else if (dataVer > itemVer)
        return m_data_vec[m_keys_vec[threadData.m_key_idx].getM_data_index().load()];
    else
        // same version - return latest key_idx by order in order-array
        return key_idx > threadData.m_key_idx ?
               m_data_vec[m_keys_vec[key_idx].getM_data_index().load()] :
               m_data_vec[m_keys_vec[threadData.m_key_idx].getM_data_index().load()];
}


template<typename K>
int Chunk<K>::baseAllocateSerial(int dataSize) {

    uint key_index = m_keys_index_serial;
    m_keys_index_serial.fetch_add(1);

    if (key_index >= m_keys_vec.size() || dataSize == -1)
        return -1;

    // if there's data - allocate room for it
    // otherwise DATA field of order-item is left as KIWI_NONE
    // increment data array to get new index in it
    uint data_index = m_data_index_serial;
    if (data_index >= m_data_vec.size())
        return -1;


    m_data_index_serial.fetch_add(1);

    // write base item data location (offset of data-array) to order-array
    // since KIWI_NONE==0, item's version and next are already set to KIWI_NONE
    m_keys_vec[key_index].getM_data_index().store(data_index);

    // return index of allocated order-array item
    return key_index;

}


template<typename K>
int Chunk<K>::allocateSerial(KeyCell<K> key, int data) {
    // allocate items in order and data array => data-array only contains int-sized data
    int key_index = baseAllocateSerial(data == -1 ? 0 : 1);

    if (key_index >= 0) {
        // write integer key into (int) order array at correct offset
        m_keys_vec[key_index].getM_key().store(key.getM_key());

        if (data != -1) {
            int data_index = m_keys_vec[key_index].getM_data_index();
            m_data_vec[data_index] = data;
        }
    }

    // return order-array index (can be used to get data-array index)
    return key_index;
}


template<typename K>
void Chunk<K>::appendItem(KeyCell<K> keyCell, int value, int version) {
    int key_index_dest = allocateSerial(keyCell, value);

    // update to item's version (since allocation gives KIWI_NONE version)
    // version is positive so item is marked as linked
    m_keys_vec[key_index_dest].getM_version() = version;
    // update binary searches range
    m_sorted_count++;

    // handle adding of first item to empty Chunk<K> *
    int prev = key_index_dest - 1;
    if (prev < 0) {
        m_keys_vec[HEAD_NODE].getM_next_key_offset().store(key_index_dest);
        return;
    }

    // updated dest Chunk<K> *'s linked list with new item
    m_keys_vec[prev].getM_next_key_offset().store(key_index_dest);
}


template<typename K>
int Chunk<K>::copyPart(Chunk<K> *srcchunk, int key_index, int maxCapacity, ScanIndex<K> scanIndex) {
    int maxIdx = maxCapacity + 1;

    if (m_keys_index_serial >= maxIdx) return key_index;

    if (m_keys_index_serial != FIRST_INDEX) {
        m_keys_vec[m_keys_index_serial - 1].getM_next_key_offset().store(m_keys_index_serial);
    } else {
        m_keys_vec[HEAD_NODE].getM_next_key_offset().store(FIRST_INDEX);
    }

    int sortedSize = srcchunk->m_sorted_count + 1;
    int key_index_start = key_index;
    int key_index_end = key_index_start - 1;

    KeyCell<K> currKey = *KeyCell<K>::getEmpty_KeyCell();
    KeyCell<K> prevKey = *KeyCell<K>::getEmpty_KeyCell();

    int currDataId = KIWI_NONE;
    int prevDataId = KIWI_NONE;

    int currVersion = KIWI_NONE;

    int key_index_prev = KIWI_NONE;

    bool isFirst = true;

    while (true) {
        currKey = srcchunk->m_keys_vec[key_index];
        currDataId = srcchunk->m_keys_vec[key_index].getM_data_index();

        int itemsToCopy = key_index_end - key_index_start + 1;

        if ((currDataId > 0) &&
            (isFirst||
             (
                     (key_index_prev < sortedSize)
                     &&
                     (!(prevKey == currKey))
                     &&
                     (key_index_prev + 1 == key_index)
                     &&
                     (m_keys_index_serial + itemsToCopy <= maxIdx)
                     &&
                     (prevDataId + 1 == currDataId)
             ))) {
            key_index_end++;
            isFirst = false;

            prevKey = currKey;
            key_index_prev = key_index;
            key_index = srcchunk->m_keys_vec[key_index].getM_next_key_offset();
            prevDataId = currDataId;

            if (key_index != KIWI_NONE) continue;

        }

        // copy continuous interval by arrayCopy
        itemsToCopy = key_index_end - key_index_start + 1;
        //todo understand why this is commented out what was being copied here and why we don't need it
        //System.arraycopy(srcchunk.orderArray, key_index_start, orderArray, m_keys_index_serial, itemsToCopy*ORDER_SIZE );
        if (itemsToCopy > 0) {
            for (int i = 0; i < itemsToCopy; ++i) {
                int offset = i;
                int current_key_idx = m_keys_index_serial;

                // next should point to the next item
                m_keys_vec[current_key_idx + offset].getM_next_key_offset() = current_key_idx + offset + 1;
                m_keys_vec[current_key_idx + offset].getM_version() = std::abs(
                        srcchunk->m_keys_vec[key_index_start + offset].getM_version().load());
                m_keys_vec[current_key_idx + offset].getM_data_index() = m_data_index_serial + i;
                m_keys_vec[current_key_idx + offset].getM_key() = srcchunk->m_keys_vec[key_index_start +
                                                                                            offset].getM_key().load();
            }

            m_keys_index_serial = m_keys_index_serial + itemsToCopy;

            int dataIdx = srcchunk->m_keys_vec[key_index_start].getM_data_index();

            if (itemsToCopy == 1) {
                m_data_vec[m_data_index_serial] = srcchunk->m_data_vec[dataIdx];
            } else {
                for (int i = m_data_index_serial; i < m_data_index_serial + itemsToCopy; ++i) {
                    m_data_vec[i] = srcchunk->m_data_vec[dataIdx];
                }
            }

            m_data_index_serial = m_data_index_serial + itemsToCopy;
        }

        scanIndex.reset(currKey);
        //first item already copied or null
        scanIndex.savedVersion();

        currVersion = srcchunk->m_keys_vec[key_index].getM_version();

        int removedVersion = KIWI_NONE;

        // the case when we start from deleted item
        if (!(prevKey == currKey) && currDataId < 0) {
            // remove the item if it doesn't have versions to keep
            removedVersion = currVersion;

            // move to next item
            key_index_prev = key_index;
            prevDataId = currDataId;

            key_index = srcchunk->m_keys_vec[key_index].getM_next_key_offset();
            currDataId = srcchunk->m_keys_vec[key_index].getM_data_index();

            currVersion = srcchunk->m_keys_vec[key_index].getM_version();
            prevKey = currKey;
            currKey = srcchunk->m_keys_vec[key_index];
        }

        // copy versions of currKey if required by scanIndex, or skip to next key
        while (key_index != KIWI_NONE && prevKey == currKey) {
            if (scanIndex.shouldKeep(currVersion)) {
                if (currDataId < 0) {
                    removedVersion = currVersion;
                    scanIndex.savedVersion();
                } else if (currVersion != removedVersion) {
                    if (removedVersion != KIWI_NONE) {
                        //todo assume -1 is no value
                        appendItem(currKey, -1, removedVersion);
                        m_keys_vec[m_keys_index_serial - 1].getM_next_key_offset().store(m_keys_index_serial);
                        scanIndex.savedVersion();
                        removedVersion = KIWI_NONE;
                    }


                    appendItem(currKey, srcchunk->m_data_vec[currDataId], currVersion);
                    m_keys_vec[m_keys_index_serial - 1].getM_next_key_offset().store(m_keys_index_serial);
                    scanIndex.savedVersion();
                }
            }

            key_index_prev = key_index;
            prevDataId = currDataId;

            key_index = srcchunk->m_keys_vec[key_index].getM_next_key_offset();
            currDataId = srcchunk->m_keys_vec[key_index].getM_data_index();

            currVersion = srcchunk->m_keys_vec[key_index].getM_version();
            prevKey = currKey;
            currKey = srcchunk->m_keys_vec[key_index];
        }

        if (key_index == KIWI_NONE || m_keys_index_serial > maxIdx)
            break;


        key_index_start = key_index;
        key_index_end = key_index_start - 1;
        isFirst = true;
    }


    int setIdx = m_keys_index_serial > FIRST_INDEX ? m_keys_index_serial - 1 : HEAD_NODE;
    m_keys_vec[setIdx].getM_next_key_offset().store(KIWI_NONE);
    m_keys_free_index.store(m_keys_index_serial);
    m_data_index_serial.store(m_data_index_serial);
    m_sorted_count.store(m_keys_index_serial);

    return key_index;
}


template<typename K>
Chunk<K> *Chunk<K>::newChunk (KeyCell<K> minKey) {
    return new Chunk<K>(minKey.clone(), this);
}


template<typename K>
int Chunk<K>::baseAllocate(int dataSize) {
    // increment order array to get new index in it
    int key_index = m_keys_free_index.fetch_add(1);

    if(key_index < 0)
        return -2;

    if ((uint) key_index + 1 > m_keys_vec.size())
        return -1;

    // increment data array to get new index in it
    int data_index = m_data_free_index.fetch_add(1);
    if ((uint)data_index >= m_data_vec.size())
        return -1;

    // if there's data - allocate room for it
    // otherwise DATA field of order-item is left as KIWI_NONE
    // write base item data location (offset of data-array) to order-array
    // since KIWI_NONE==0, item's version and next are already set to KIWI_NONE
    // todo data index negative?
    data_index = dataSize > 0 ? data_index : -data_index;

    m_keys_vec[key_index].getM_data_index().store(data_index);

    // return index of allocated order-array item
    return key_index;
}


template<typename K>
int Chunk<K>::allocate(KeyCell<K> key, int data) {

    // allocate items in order and data array => data-array only contains int-sized data
    int key_index = baseAllocate(data == -1 ? 0 : 1);


    if (key_index >= 0) {

        // write integer key into (int) order array at correct offset
        m_keys_vec[key_index].getM_key().store(key.getM_key());


        // get data index
        if (data != -1) {
            int di = m_keys_vec[key_index].getM_data_index();

            m_data_vec[di] = data;

        }

    }

    // return order-array index (can be used to get data-array index)
    return key_index;
}


template<typename K>
std::map<KeyCell<K>, ThreadData> *Chunk<K>::helpPutInScan(int myVersion, KeyCell<K> min, KeyCell<K> max) {
    std::map<KeyCell<K>, ThreadData> *items = new std::map<KeyCell<K>, ThreadData>();

    // go over thread data of all threads
    for (int i = 0; i < MAX_THREADS; ++i) {
        // make sure data is for a Put operatio
        ThreadData currPut = m_put_array_queue[i];
        //todo we assume that put with -1 is empty
        if (currPut.m_key_idx == -1)
            continue;

        // if put operation's key is not in key range - skip it
        KeyCell<K> currKey = m_keys_vec[currPut.m_key_idx];
        if ((currKey < min || max < currKey))
            continue;

        // read the current version of the item
        int currVer = currKey.getM_version();

        // if empty, try to set to my version
        if (currVer == KIWI_NONE)
            currVer = set_version(currPut.m_key_idx, myVersion);

        // if item is frozen or beyond my version - skip it
        if ((currVer == FREEZE_VERSION) || (currVer > myVersion))
            continue;

        // get found item matching current key
        auto item = items->find(currKey);

        // is there such an item we previously found? check if we need to replace it
        if (item != items->end()) {
            // get its version
            int itemVer = m_keys_vec[item->second.m_key_idx].getM_version();

            // existing item is newer - don't replace
            if (itemVer > currVer) {
                continue;
            }
                // if same versions - continue checking (otherwise currVer is newer so will replace item)
            else if (itemVer == currVer) {
                // same Chunk<K> *& version but items's index is larger - don't replace
                if (item->second.m_key_idx > currPut.m_key_idx)
                    continue;
            }

        }

        // if we've reached here then curr is newer than item, and we replace it
        (*items)[currKey] = currPut;
    }

    return items;
}


template<typename K>
int Chunk<K>::copyValues(std::vector<int> *result, int idx, int myVer, KeyCell<K> min, KeyCell<K> max) {
    int key_index = 0;

    if (idx == 0) {
        key_index = findFirst(min, myVer);
        if (key_index == KIWI_NONE) return 0;

    } else {
        key_index = getFirst(myVer);
    }

    int sortedSize = m_sorted_count;
    KeyCell<K> prevKey = *KeyCell<K>::getEmpty_KeyCell();
    KeyCell<K> currKey = *KeyCell<K>::getEmpty_KeyCell();
    KeyCell<K> maxKey = max;

    int dataStart = m_keys_vec[key_index].getM_data_index();
    int dataEnd = dataStart - 1;
    int itemCount = 0;
    int key_index_prev = KIWI_NONE;
    int prevDataId = KIWI_NONE;
    int currDataId = dataStart;


    bool isFirst = dataStart > 0 ? true : false;

    while (key_index != KIWI_NONE) {

        if (max < currKey)
            break;

        if (isFirst ||
            (
                    (key_index_prev < sortedSize)
                    &&
                    (!(prevKey == currKey))
                    &&
                    currKey <= maxKey
                    &&
                    (prevDataId + 1 == currDataId)
                    &&
                    (m_keys_vec[key_index].getM_version() <= myVer)
            )) {
            if (isFirst) {
                dataEnd++;
                isFirst = false;
            } else {
                dataEnd = currDataId < 0 ? dataEnd : dataEnd + 1;
            }

            key_index_prev = key_index;
            key_index = m_keys_vec[key_index].getM_next_key_offset();

            prevKey = currKey;
            prevDataId = currDataId;
            currKey = m_keys_vec[key_index];
            currDataId = currKey.getM_data_index();

        } else {
            // copy continuous range of data
            int itemsToCopy = dataEnd - dataStart + 1;

            if (itemsToCopy == 1) {
                result->at(idx + itemCount) = m_data_vec[dataStart];
            } else if (itemsToCopy > 1) {
                for (int i = 0; i < itemsToCopy; ++i) {
                    result->at(idx + i + itemCount) = m_data_vec[dataStart + i];
                }
            }

            itemCount += itemsToCopy;

            // find next item to start copy interval
            while (key_index != KIWI_NONE) {

                if (max < currKey) return itemCount;

                // if in a valid version, and a different key - found next item
                if ((!(prevKey == currKey)) && (m_keys_vec[key_index].getM_version() <= myVer)) {
                    if (currDataId < 0) {
                        // the value is NULL, the item was removed -- skip it
                        prevKey = currKey;

                    } else {
                        break;
                    }
                }

                // otherwise proceed to next

                key_index_prev = key_index;
                key_index = m_keys_vec[key_index].getM_next_key_offset();

                prevDataId = currDataId;
                currDataId = currKey.getM_data_index();
            }

            if (key_index == KIWI_NONE) return itemCount;

            dataStart = currDataId;//get(key_index, OFFSET_DATA);
            dataEnd = dataStart - 1;
            isFirst = true;
        }

    }

    int itemsToCopy = dataEnd - dataStart + 1;
    for (int i = 0; i < itemsToCopy; ++i) {
        result->at(idx + i + itemCount) = m_data_vec[dataStart + i];
    }

    itemCount += itemsToCopy;


    return itemCount;
}



