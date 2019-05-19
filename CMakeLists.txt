cmake_minimum_required(VERSION 3.5)
set(TARGET_NAME KiWi_cpp_pq_port)

project(${TARGET_NAME})

option(HAVE_FOLLY "Link with folly implementation" OFF)
option(HAVE_GALOIS "Link with Galois implementation" OFF)

set(FOLLY_INC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/folly/include)
set(FOLLY_LIB_DIR ${CMAKE_CURRENT_SOURCE_DIR}/folly/lib)
set(GALOIS_INC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/galois/include)
set(GALOIS_LIB_DIR ${CMAKE_CURRENT_SOURCE_DIR}/galois/lib)
#set (HAVE_FOLLY 1)
#set (HAVE_GALOIS 1)

include_directories(BEFORE SYSTEM /specific/disk1/home/mad/Galois-PQ/inputs/students/ofirc1/glog/install/usr/include)
link_directories(/specific/disk1/home/mad/Galois-PQ/inputs/students/ofirc1/glog/install/usr/local/lib)

link_directories(${FOLLY_LIB_DIR})
link_directories(${GALOIS_LIB_DIR})

string(APPEND CMAKE_CXX_FLAGS " -pthread")


find_package(Boost 1.54.0 COMPONENTS system filesystem REQUIRED)
if(Boost_FOUND)
    message(STATUS "Boost_INCLUDE_DIRS: ${Boost_INCLUDE_DIRS}")
    message(STATUS "Boost_LIBRARIES: ${Boost_LIBRARIES}")
    message(STATUS "Boost_VERSION: ${Boost_VERSION}")
endif()

set(CMAKE_CXX_STANDARD 14)
#set(TESTER main.cpp)
set(TESTER test_dequeue.cpp)

set(KIWI_SOURCES ${TESTER} Chunk.hpp atomicMarkableReference.hpp
                 Statistics.hpp KeyCell.hpp ThreadData.hpp
                 KiWi.hpp config.hpp ItemsIterator.hpp
                 VersionsIterator.hpp
                 ScanIndex.hpp Rebalancer.hpp
                 Compactor.hpp
                 MultiChunkIterator.hpp ScanData.hpp Statistics.hpp
                 PolicyImpl.hpp ctpl_stl.h SkipListNodeDecl.h KiWiChunkSize.cpp)

add_library(kiwi STATIC ${KIWI_SOURCES})

# TODO: restore these 2 lines.
add_executable(${TARGET_NAME} ${KIWI_SOURCES})
target_link_libraries(${TARGET_NAME} kiwi)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
target_link_libraries(${TARGET_NAME} pthread boost_timer)

if(Boost_FOUND)
#  target_include_directories(${TARGET_NAME} PUBLIC ${Boost_INCLUDE_DIRS})
endif()

if(HAVE_FOLLY)
target_link_libraries(${TARGET_NAME} folly kiwi)
target_include_directories(kiwi PUBLIC ${FOLLY_INC_DIR})
target_compile_definitions(kiwi PUBLIC HAVE_FOLLY=1)

target_link_libraries(${TARGET_NAME} kiwi folly glog double-conversion dl)
target_include_directories(${TARGET_NAME} PUBLIC ${FOLLY_INC_DIR})
target_compile_definitions(${TARGET_NAME} PUBLIC HAVE_FOLLY=1)
endif()

if(HAVE_GALOIS)
target_include_directories(kiwi PUBLIC ${GALOIS_INC_DIR})
target_compile_definitions(kiwi PUBLIC HAVE_GALOIS=1)
target_link_libraries(${TARGET_NAME} galois numa)
endif()
