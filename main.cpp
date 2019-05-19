#include <iostream>
#include "KiWi.hpp"
#include "ctpl_stl.h"
#include <folly/ConcurrentSkipList.h>

#include <ctime>

//typedef ConcurrentSkipList<SkipListNode, SkipListNodeComparator> SkipListT;

int random(int min, int max) //range : [min, max)
{
    static bool first = true;
    if (first)
    {
        srand( time(NULL) ); //seeding for the first time only!
        first = false;
    }
    return min + rand() % (( max + 1 ) - min);
}



int main() {
    std::clock_t start;
    double duration;
    start = std::clock();

    std::unique_ptr<Chunk<int>> first_chunk = std::make_unique<Chunk<int>>(KeyCell<int>(), nullptr);
    std::unique_ptr<KiWi<int>> k = std::make_unique<KiWi<int>>(first_chunk.get(),true);
    int iters = 500000;

//    for (int i = 1; i < iters; ++i) {
//            k->put(KeyCell(i,i*2,0,0,0),50);
//    }

    std::vector<int> inputs;
    std::vector<int> outputs;
    for (int i = 1; i < iters; ++i) {
        inputs.push_back(i);
    }
//    for (int i = 1; i < iters; ++i) {
//        int rand = random(0,inputs.size()-1);
//        int t = inputs.at(rand);
//        inputs.erase(inputs.begin() + rand);
//        k->put(KeyCell(t,t*2,0,0,0),50);
//    }

//    {
//        ctpl::thread_pool p(16 /* two threads in the pool */);
//
//        for (int i = 1; i < iters; ++i) {
//            if(i % 10000 == 0)
//                std::cout << i  << std::endl;
//            if(random(0,1) == 0){
//                int rand = random(0,inputs.size()-1);
//                int t = inputs.at(rand);
//                outputs.push_back(t);
//                inputs.erase(inputs.begin() + rand);
//                p.push([&k,t](int){ k->put(KeyCell(t,t*2,0,0,0),50);});
//
//            }else if(outputs.size() > 0){
//                int rand = random(0,outputs.size()-1);
//                int t = outputs.at(rand);
//                outputs.erase(outputs.begin() + rand);
//                p.push([&k,t](int){ auto v = k->get(KeyCell(t,t*2,0,0,0));
//                                    std::cout << v << std::endl;});
//            }
//        }
//    }

    auto v = k->get(KeyCell<int>(1500,0,0,0));
    std::cout << v << std::endl;

    {
        ctpl::thread_pool p(MAX_THREADS /* two threads in the pool */);

        for (int i = 1; i < iters; ++i) {
//            if(i % 10000 == 0)
//                std::cout << i  << std::endl;
            if(random(0,1) == 0){
                int rand = random(0,inputs.size()-1);
                int t = inputs.at(rand);
                outputs.push_back(t);
                inputs.erase(inputs.begin() + rand);
                p.push([&k,t](int){ k->put(KeyCell<int>(t,0,0,0),50);});

            }else if(outputs.size() > 0){
                int rand = random(0,outputs.size()-1);
                int t = outputs.at(rand);
                outputs.erase(outputs.begin() + rand);
                p.push([&k,t](int){ auto v = k->dequeue();
                    //std::cout << v.getM_key() << std::endl;
                    });
            }
        }
    }

//    typename SkipListT::Accessor accessor(k->m_skiplist);

    duration = ( std::clock() - start ) / (double) CLOCKS_PER_SEC;

    std::cout<<"timer : "<< duration <<'\n';

    return 0;
}