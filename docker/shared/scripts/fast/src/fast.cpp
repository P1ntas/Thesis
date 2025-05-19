#include "fast.hpp"
#include <sys/mman.h>
#include <algorithm>
#include <cassert>
#include <climits>
#include <iostream>

namespace fast {

template<unsigned K>
FastTree<K>::FastTree() : tree_data_(nullptr), tree_size_(0), data_size_(0), scale_(0) {
    for (unsigned i = 0; i < K; i++) {
        scale_ += pow16(i);
    }
    scale_ *= 16;
}

template<unsigned K>
FastTree<K>::~FastTree() {
    if (tree_data_) {
        munmap(tree_data_, tree_size_ * sizeof(int32_t));
    }
}

template<unsigned K>
void* FastTree<K>::malloc_huge(size_t size) {
    void* p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
#if __linux__
    madvise(p, size, MADV_HUGEPAGE);
#endif
    return p;
}

template<unsigned K>
inline void FastTree<K>::storeSIMDblock(int32_t v[], unsigned k,
                                        const std::vector<std::pair<int32_t, size_t>>& entries,
                                        unsigned i, unsigned j) {
    unsigned m = median(i, j);
    v[k + 0] = entries[m].first;
    v[k + 1] = entries[median(i, m)].first;
    v[k + 2] = entries[median(1 + m, j)].first;
}

template<unsigned K>
inline unsigned FastTree<K>::storeCachelineBlock(int32_t v[], unsigned k,
                                                 const std::vector<std::pair<int32_t, size_t>>& entries,
                                                 unsigned i, unsigned j) {
    storeSIMDblock(v, k + 3 * 0, entries, i, j);
    unsigned m = median(i, j);
    storeSIMDblock(v, k + 3 * 1, entries, i, median(i, m));
    storeSIMDblock(v, k + 3 * 2, entries, median(i, m) + 1, m);
    storeSIMDblock(v, k + 3 * 3, entries, m + 1, median(m + 1, j));
    storeSIMDblock(v, k + 3 * 4, entries, median(m + 1, j) + 1, j);
    return k + 16;
}

template<unsigned K>
unsigned FastTree<K>::storeFASTpage(int32_t v[], unsigned offset,
                                    const std::vector<std::pair<int32_t, size_t>>& entries,
                                    unsigned i, unsigned j, unsigned levels) {
    for (unsigned level = 0; level < levels; level++) {
        unsigned chunk = (j - i) / pow16(level);
        for (unsigned cl = 0; cl < pow16(level); cl++) {
            offset = storeCachelineBlock(v, offset, entries, i + cl * chunk, i + (cl + 1) * chunk);
        }
    }
    return offset;
}

template<unsigned K>
inline unsigned FastTree<K>::maskToIndex(unsigned bitmask) const {
    static unsigned table[8] = {0, 9, 1, 2, 9, 9, 9, 3};
    return table[bitmask & 7];
}

template<unsigned K>
unsigned FastTree<K>::searchInternal(int32_t key_q) const {
    __m128i xmm_key_q = _mm_set1_epi32(key_q);
    
    unsigned page_offset = 0;
    unsigned level_offset = 0;
    
    for (unsigned cl_level = 1; cl_level <= 4; cl_level++) {
        __m128i xmm_tree = _mm_loadu_si128((__m128i*)(tree_data_ + page_offset + level_offset * 16));
        __m128i xmm_mask = _mm_cmpgt_epi32(xmm_key_q, xmm_tree);
        unsigned index = _mm_movemask_ps(_mm_castsi128_ps(xmm_mask));
        unsigned child_index = maskToIndex(index);
        
        xmm_tree = _mm_loadu_si128((__m128i*)(tree_data_ + page_offset + level_offset * 16 + 3 + 3 * child_index));
        xmm_mask = _mm_cmpgt_epi32(xmm_key_q, xmm_tree);
        index = _mm_movemask_ps(_mm_castsi128_ps(xmm_mask));
        
        unsigned cache_offset = child_index * 4 + maskToIndex(index);
        level_offset = level_offset * 16 + cache_offset;
        page_offset += pow16(cl_level);
    }
    
    unsigned pos = level_offset;
    unsigned offset = 69904 + level_offset * scale_;
    page_offset = 0;
    level_offset = 0;
    
    for (unsigned cl_level = 1; cl_level <= K; cl_level++) {
        __m128i xmm_tree = _mm_loadu_si128((__m128i*)(tree_data_ + offset + page_offset + level_offset * 16));
        __m128i xmm_mask = _mm_cmpgt_epi32(xmm_key_q, xmm_tree);
        unsigned index = _mm_movemask_ps(_mm_castsi128_ps(xmm_mask));
        unsigned child_index = maskToIndex(index);
        
        xmm_tree = _mm_loadu_si128((__m128i*)(tree_data_ + offset + page_offset + level_offset * 16 + 3 + 3 * child_index));
        xmm_mask = _mm_cmpgt_epi32(xmm_key_q, xmm_tree);
        index = _mm_movemask_ps(_mm_castsi128_ps(xmm_mask));
        
        unsigned cache_offset = child_index * 4 + maskToIndex(index);
        level_offset = level_offset * 16 + cache_offset;
        page_offset += pow16(cl_level);
    }
    
    return (pos << (K * 4)) | level_offset;
}

template<>
template<>
int32_t FastTree<3>::dateToInt32<std::chrono::system_clock::time_point>(
    const std::chrono::system_clock::time_point& date) const {
    auto epoch = date.time_since_epoch();
    auto seconds = std::chrono::duration_cast<std::chrono::seconds>(epoch);
    return static_cast<int32_t>(seconds.count() / (24 * 60 * 60));
}

template<>
template<>
std::chrono::system_clock::time_point FastTree<3>::int32ToDate<std::chrono::system_clock::time_point>(
    int32_t value) const {
    auto days = std::chrono::seconds(value * 24 * 60 * 60);
    return std::chrono::system_clock::time_point(days);
}

template<>
template<>
int32_t FastTree<3>::dateToInt32<uint64_t>(const uint64_t& date) const {
    return static_cast<int32_t>(date / (24 * 60 * 60 * 1000));
}

template<>
template<>
uint64_t FastTree<3>::int32ToDate<uint64_t>(int32_t value) const {
    return static_cast<uint64_t>(value) * 24 * 60 * 60 * 1000;
}

template<>
template<>
int32_t FastTree<3>::dateToInt32<int32_t>(const int32_t& date) const {
    return date;
}

template<>
template<>
int32_t FastTree<3>::int32ToDate<int32_t>(int32_t value) const {
    return value;
}

template<unsigned K>
template<typename DateType, typename ValueType>
void FastTree<K>::build(const std::vector<Entry<DateType, ValueType>>& entries) {
    data_size_ = entries.size();
    
    key_to_index_.clear();
    key_to_index_.reserve(entries.size());
    
    for (size_t i = 0; i < entries.size(); ++i) {
        int32_t date_int = dateToInt32(entries[i].date);
        key_to_index_.emplace_back(date_int, i);
    }
    
    std::sort(key_to_index_.begin(), key_to_index_.end());
    
    unsigned n = 1u << (16 + (K * 4));
    while (key_to_index_.size() < n) {
        key_to_index_.emplace_back(INT_MAX, SIZE_MAX);
    }
    
    tree_size_ = 0;
    for (unsigned i = 0; i < K + 4; i++) {
        tree_size_ += pow16(i);
    }
    tree_size_ = tree_size_ * 64 / 4;
    
    tree_data_ = static_cast<int32_t*>(malloc_huge(sizeof(int32_t) * tree_size_));
    
    unsigned offset = storeFASTpage(tree_data_, 0, key_to_index_, 0, n, 4);
    unsigned chunk = n / (1u << 16);
    for (unsigned i = 0; i < (1u << 16); i++) {
        offset = storeFASTpage(tree_data_, offset, key_to_index_, i * chunk, (i + 1) * chunk, K);
    }
    assert(offset == tree_size_);
}

template<unsigned K>
template<typename DateType, typename ValueType>
size_t FastTree<K>::search(const DateType& date) const {
    if (!tree_data_) return SIZE_MAX;
    
    int32_t date_int = dateToInt32(date);
    unsigned result = searchInternal(date_int);

    return result;
}

template<unsigned K>
template<typename DateType, typename ValueType>
typename FastTree<K>::template RangeResult<DateType, ValueType> 
FastTree<K>::rangeLessThan(const DateType& cutoff,
                          const std::vector<Entry<DateType, ValueType>>& original_data) const {
    RangeResult<DateType, ValueType> result;
    
    if (!tree_data_ || key_to_index_.empty()) {
        return result;
    }
    
    int32_t cutoff_int = dateToInt32(cutoff);
    
    auto it = std::lower_bound(key_to_index_.begin(), key_to_index_.end(),
                              std::make_pair(cutoff_int, size_t(0)));
    
    for (auto iter = key_to_index_.begin(); iter != it; ++iter) {
        if (iter->second != SIZE_MAX && iter->second < original_data.size()) {
            result.entries.push_back(original_data[iter->second]);
            result.count++;
        }
    }
    
    return result;
}

template<unsigned K>
template<typename DateType, typename ValueType>
typename FastTree<K>::template RangeResult<DateType, ValueType>
FastTree<K>::rangeSearch(const DateType& start,
                        const DateType& end,
                        const std::vector<Entry<DateType, ValueType>>& original_data) const {
    RangeResult<DateType, ValueType> result;
    
    if (!tree_data_ || key_to_index_.empty()) {
        return result;
    }
    
    int32_t start_int = dateToInt32(start);
    int32_t end_int = dateToInt32(end);
    
    auto start_it = std::lower_bound(key_to_index_.begin(), key_to_index_.end(),
                                    std::make_pair(start_int, size_t(0)));
    auto end_it = std::upper_bound(key_to_index_.begin(), key_to_index_.end(),
                                  std::make_pair(end_int, SIZE_MAX));
    
    for (auto iter = start_it; iter != end_it; ++iter) {
        if (iter->second != SIZE_MAX && iter->second < original_data.size()) {
            result.entries.push_back(original_data[iter->second]);
            result.count++;
        }
    }
    
    return result;
}

template<unsigned K>
template<typename DateType, typename ValueType>
typename FastTree<K>::template RangeResult<DateType, ValueType>
FastTree<K>::rangeGreaterThan(
    const DateType& cutoff,
    const std::vector<Entry<DateType, ValueType>>& original_data) const
{
    RangeResult<DateType, ValueType> result;
    if (!tree_data_ || key_to_index_.empty()) return result;

    int32_t cutoff_int = dateToInt32(cutoff);
    auto it = std::upper_bound(
        key_to_index_.begin(), key_to_index_.end(),
        std::make_pair(cutoff_int, SIZE_MAX)
    );

    for (auto iter = it; iter != key_to_index_.end(); ++iter) {
        if (iter->second != SIZE_MAX && iter->second < original_data.size()) {
            result.entries.push_back(original_data[iter->second]);
            result.count++;
        }
    }
    return result;
}


template<unsigned K>
size_t FastTree<K>::getMemoryUsage() const {
    size_t memory = tree_size_ * sizeof(int32_t);
    memory += key_to_index_.size() * sizeof(std::pair<int32_t, size_t>); 
    return memory;
}

template class FastTree<1>;
template class FastTree<2>;
template class FastTree<3>;
template class FastTree<4>;

template void FastTree<3>::build<std::chrono::system_clock::time_point, uint64_t>(
    const std::vector<Entry<std::chrono::system_clock::time_point, uint64_t>>&);

template void FastTree<3>::build<uint64_t, uint64_t>(
    const std::vector<Entry<uint64_t, uint64_t>>&);

template void FastTree<3>::build<int32_t, uint64_t>(
    const std::vector<Entry<int32_t, uint64_t>>&);

template size_t FastTree<3>::search<std::chrono::system_clock::time_point, uint64_t>(
    const std::chrono::system_clock::time_point&) const;

template size_t FastTree<3>::search<uint64_t, uint64_t>(
    const uint64_t&) const;

template size_t FastTree<3>::search<int32_t, uint64_t>(
    const int32_t&) const;

template typename FastTree<3>::RangeResult<std::chrono::system_clock::time_point, uint64_t>
FastTree<3>::rangeLessThan<std::chrono::system_clock::time_point, uint64_t>(
    const std::chrono::system_clock::time_point&,
    const std::vector<Entry<std::chrono::system_clock::time_point, uint64_t>>&) const;

template typename FastTree<3>::RangeResult<uint64_t, uint64_t>
FastTree<3>::rangeLessThan<uint64_t, uint64_t>(
    const uint64_t&,
    const std::vector<Entry<uint64_t, uint64_t>>&) const;

template typename FastTree<3>::RangeResult<int32_t, uint64_t>
FastTree<3>::rangeLessThan<int32_t, uint64_t>(
    const int32_t&,
    const std::vector<Entry<int32_t, uint64_t>>&) const;

template typename FastTree<3>::RangeResult<std::chrono::system_clock::time_point, uint64_t>
FastTree<3>::rangeSearch<std::chrono::system_clock::time_point, uint64_t>(
    const std::chrono::system_clock::time_point&,
    const std::chrono::system_clock::time_point&,
    const std::vector<Entry<std::chrono::system_clock::time_point, uint64_t>>&) const;

template typename FastTree<3>::RangeResult<uint64_t, uint64_t>
FastTree<3>::rangeSearch<uint64_t, uint64_t>(
    const uint64_t&,
    const uint64_t&,
    const std::vector<Entry<uint64_t, uint64_t>>&) const;

template typename FastTree<3>::RangeResult<int32_t, uint64_t>
FastTree<3>::rangeSearch<int32_t, uint64_t>(
    const int32_t&,
    const int32_t&,
    const std::vector<Entry<int32_t, uint64_t>>&) const;

template typename FastTree<3>::RangeResult<int32_t, uint64_t>
FastTree<3>::rangeGreaterThan<int32_t, uint64_t>(
    const int32_t&,
    const std::vector<Entry<int32_t, uint64_t>>&) const;
} 
