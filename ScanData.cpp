//
// Created by dvir on 10/02/19.
//

#include "ScanData.hpp"

const ScanData* ScanData::empty_ScanData = new ScanData(*KeyCell::getEmpty_KeyCell(), *KeyCell::getEmpty_KeyCell());

