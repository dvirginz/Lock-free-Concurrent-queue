//
// Created by dvir on 05/02/19.
//

template<typename K>
bool ScanIndex<K>::shouldKeep(int version) {

    //always save the first provided version.
    if(isFirst) return true;
    if(index >= m_numOfVersions) return false;

    if(currKey < m_fromKeys[index] ) return false;
    if(currKey < m_toKeys[index]) return false;

    return m_scanVersions[index] >= version;

}

template<typename K>
void ScanIndex<K>::savedVersion() {
    isFirst = false;
    index++;
}

template<typename K>
ScanIndex<K>::ScanIndex(std::vector<ScanData<K> *> *scans, KeyCell<K> &minKey, KeyCell<K>  &maxKey) :
m_scanVersions(scans->size()), m_fromKeys(scans->size()),m_toKeys(scans->size()),m_numOfVersions(0){

        //todo I assume priority 0 is null cell
        min_cell = (minKey.getM_key() != 0) ? minKey : *KeyCell<K> ::getEmpty_KeyCell();
        max_cell = (maxKey.getM_key() != 0) ? maxKey : *KeyCell<K> ::getEmpty_KeyCell();

        //todo what is noinspection
        //noinspection Since15
        std::sort(scans->begin(), scans->end(),
         [](const ScanData<K> *&& a, const ScanData<K> *&& b) -> bool
         {
             return (a->version.load() > b->version.load());
         });

        //for(int i = 0; i < scans->length; ++i)
        for(ScanData<K> * sd: *scans)
        {
            if(sd == nullptr) continue;
            if(sd->max < min_cell || min_cell == *KeyCell<K>::getEmpty_KeyCell()) continue;
            if(max_cell < sd->min && max_cell.getM_key() != 0) continue;

            m_scanVersions[m_numOfVersions] = sd->version;
            m_fromKeys[m_numOfVersions] = sd->min;
            m_toKeys[m_numOfVersions] = sd->max;
            m_numOfVersions++;
        }

        //todo what this line is doing? what is reset -1 is doing?
        reset(*KeyCell<K>::getEmpty_KeyCell());
    

}

template<typename K>
void ScanIndex<K>::reset(KeyCell<K> key) {
    index = -1;
    isFirst = true;
    currKey = key;
}
