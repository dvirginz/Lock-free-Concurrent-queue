//
// Created by dvir on 04/02/19.
//

template<typename K>
const KeyCell<K>* KeyCell<K>::empty_KeyCell = new KeyCell<K>(K(),0,0,0);

template<typename K>
std::string
KeyCell<K>::to_string(){
    std::stringstream to_string_ss;
    to_string_ss << "KeyCell<K>: m_key: " << m_key << " m_next_key_offset: " << m_next_key_offset;
    return to_string_ss.str();
};

template<typename K>
KeyCell<K>
KeyCell<K>::clone(){
// return new KeyCell<K> wrapping the cloned byte array
    return KeyCell<K>(*m_key,m_next_key_offset, m_data_index, m_version);
}

template<typename K>
bool KeyCell<K>::equals(const KeyCell<K> *obj) const{
    return (*m_key == *(obj->m_key) && m_next_key_offset==obj->m_next_key_offset);
}


template<typename K>
std::atomic<int> &KeyCell<K>::getM_next_key_offset() {
    return m_next_key_offset;
}

template<typename K>
std::atomic<int> &KeyCell<K>::getM_data_index() {
    return m_data_index;
}

template<typename K>
std::atomic<K*> & KeyCell<K>::getM_key() {
    return m_key;
}

template<typename K>
const KeyCell<K> *KeyCell<K>::getEmpty_KeyCell() {
    return empty_KeyCell;
}

template<typename K>
std::atomic<int> &KeyCell<K>::getM_version() {
    return m_version;
}

template<typename K>
KeyCell<K>::KeyCell(const KeyCell<K> &cell) {
    this->m_version = cell.m_version.load();
    this->m_data_index = cell.m_data_index.load();
    this->m_key = new K(*(cell.m_key.load()));
    this->m_next_key_offset = cell.m_next_key_offset.load();
    this->m_is_valid = cell.m_is_valid.load();

}

template<typename K>
bool KeyCell<K>::operator==(const KeyCell<K> &cell) const {
    return *(this->m_key.load()) == *(cell.m_key.load());
}

template<typename K>
bool KeyCell<K>::operator<(const KeyCell<K> &keyCell) const {
    return (*(this->m_key) < *(keyCell.m_key) ||
            (*(this->m_key) == *(keyCell.m_key))) ? true : false;
}

template<typename K>
bool KeyCell<K>::operator<=(const KeyCell<K> &KeyCell) const {
    return (*this < KeyCell || *this == KeyCell);
}

template<typename K>
KeyCell<K> &KeyCell<K>::operator=(KeyCell<K> other) {
    m_data_index = other.m_data_index.load();
    m_key = new K(*other.m_key.load());
    m_next_key_offset = other.m_next_key_offset.load();
    m_version = other.m_version.load();
    m_is_valid = other.m_is_valid.load();

    return *this;
}
