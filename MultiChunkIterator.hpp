//
// Created by dvir on 07/02/19.
//

#ifndef KIWI_CPP_PQ_PORT_MULTICHUNKITERATOR_HPP
#define KIWI_CPP_PQ_PORT_MULTICHUNKITERATOR_HPP


#include "Chunk.hpp"
#include "ItemsIterator.hpp"

template<typename K>
class MultiChunkIterator {
public:

         Chunk<K>* first;
         Chunk<K>* last;
         Chunk<K>* current;
         ItemsIterator<K>* iterCurrItem;
         bool hasNextInChunk;

         MultiChunkIterator() {}

        /***
         *
         * @param chunks - Range of chunks to be iterated
         */
        MultiChunkIterator(std::vector<Chunk<K>*>* chunks);

        MultiChunkIterator(int key_index, std::vector<Chunk<K>*>* chunks);


        bool hasNext();

         Chunk<K>* findNextNonEmptyChunk();

        /**
         * After next() iterator points to some item.
         * The item's Key, Value and Version can be fetched by corresponding getters.
         */
        void next();


       KeyCell<K> getKey();


        int getValue();


        int getVersion();

        /***
         * Fetches VersionsIterator to iterate versions of current key.
         * Will help to separate versions and keys data structures in future optimizations.
         * @return VersionsIterator object
         */
        VersionsIterator<K> versionsIterator();

        /***
         *
         * @return A copy  with the same state.
         */
        MultiChunkIterator<K>* cloneIterator();

};

#include "MultiChunkIterator.cpp"

#endif //KIWI_CPP_PQ_PORT_MULTICHUNKITERATOR_HPP
