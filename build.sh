#!/bin/bash
set -e
FOLLY=""
BUILD_DIR=build

if [[ $1 == "-h" || $1 == "--help" ]]; then
    echo "Usage: $0 [folly]"
    exit 0
fi

if [[ $1 == "folly" ]]; then
    FOLLY="-DHAVE_FOLLY=1"
fi

if [[ $1 == "galois" ]]; then
    GALOIS="-DHAVE_GALOIS=1"
fi

if [[ ! -d $BUILD_DIR ]]; then
    mkdir build
fi

BOOST_PARAM=""
if [[ $HOSTNAME == *"rack-mad"* ]]; then
  BOOST_PARAM="-DBOOST_ROOT:PATHNAME=/usr/local/lib/boost_1_59_0/"
fi

pushd build >& /dev/null
cmake .. $BOOST_PARAM \
  -DCMAKE_BUILD_TYPE=Debug ${FOLLY} ${GALOIS}
cmake --build . -- -j$(grep -c processor /proc/cpuinfo)
popd >& /dev/null
