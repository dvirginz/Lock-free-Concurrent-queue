//
// Created by dvir on 04/02/19.
//

#ifndef KIWI_CPP_PQ_PORT_ATOMICMARKABLEREFERENCE_HPP
#define KIWI_CPP_PQ_PORT_ATOMICMARKABLEREFERENCE_HPP

#include <atomic>
#include <cstring>

/*
 * Struct for atomic markable reference
 */
template<class T> struct markableReference
{
    T* next;
    bool marked;

    markableReference()
    {
        next = NULL;
        marked = false;
    }

    markableReference(T* node, bool mark)
    {
        next = node;
        marked = mark;
    }

    bool operator==(const markableReference<T>& other)
    {
        return (next == other.next && marked == other.marked);
    }
};

/*
 * The AtomicMarkableReference class
 */
template<class T> class atomicMarkableReference
{
private:
    /*
     * Since a pointer is always a POD type, even if T is of non-pod type
     * atomicity will be maintained
     */
    std::atomic<markableReference<T>*> markedNext;

public:
    atomicMarkableReference()
    {
        markedNext.store(new markableReference<T> (NULL, false));
    }

    atomicMarkableReference(T* nextNode, bool mark)
    {
        /* Atomically store the values*/
        markedNext.store(new markableReference<T> (nextNode, mark));
    }

    /*
    * Returns the reference. load() is atomic and hence that will be the linearization point
    */
    T* getReference()
    {
        return markedNext.load()->next;
    }

    /*
     * Returns the reference and update bool in the reference passed as the argument
     * load() is atomic and hence that will be the linearization point.
     */
    T* get(bool *mark)
    {
        markableReference<T> *temp = markedNext.load();
        *mark = temp->marked;
        return temp->next;
    }

    bool getMark()
    {
        markableReference<T> *temp = markedNext.load();
        return temp->marked;
    }

    /*
    * Set the variables unconditionally. load() is atomic and hence that will be the linearization point
    */
    void set(T* newRef, bool newMark)
    {
        markableReference<T> *curr = markedNext.load();
        if(newRef != curr->next || newMark != curr->marked)
        {
            markedNext.store(new markableReference<T>(newRef, newMark));
        }
    }

    /*
     * CAS with reference and the marked field. load() is atomic and hence that will be the linearization point.
     * We take advantage of the fact that C++ has short-circuiting hence
     * if one of the first 2 conditions is false the atomic_compare_exchange_weak will not happen
     */

    bool CAS(T* expected, T* newValue, bool expectedBool, bool newBool)
    {
        markableReference<T> *curr = markedNext.load();
        return(expected == curr->next && expectedBool == curr->marked &&
               ((newValue == curr->next && newBool == curr->marked) ||
                markedNext.compare_exchange_strong(curr, new markableReference<T>(newValue, newBool))));
    }
};


#endif //KIWI_CPP_PQ_PORT_ATOMICMARKABLEREFERENCE_HPP
