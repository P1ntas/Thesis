#pragma once

#include <cstdint>
#include <vector>
#include <utility>
#include <emmintrin.h>
#include <chrono>
#include <memory>

namespace fast {

template<unsigned K = 3>
class FastTree {
public:
    template<typename DateType, typename ValueType>
    struct Entry {
        DateType date;
        ValueType value;
        
        Entry() = default;
        Entry(DateType d, ValueType v) : date(d), value(v) {}
    };

    template<typename DateType, typename ValueType>
    struct RangeResult {
        std::vector<Entry<DateType, ValueType>> entries;
        size_t count;
        
        RangeResult() : count(0) {}
    };

private:
    int32_t* tree_data_;
    size_t tree_size_;
    size_t data_size_;
    size_t scale_;
    
    std::vector<std::pair<int32_t, size_t>> key_to_index_;
    
    void* malloc_huge(size_t size);
    
    inline unsigned pow16(unsigned exponent) const {
        return 1u << (exponent << 2);
    }
    
    inline unsigned median(unsigned i, unsigned j) const {
        return i + (j - 1 - i) / 2;
    }
    
    inline void storeSIMDblock(int32_t v[], unsigned k, 
                              const std::vector<std::pair<int32_t, size_t>>& entries,
                              unsigned i, unsigned j);
    
    inline unsigned storeCachelineBlock(int32_t v[], unsigned k,
                                       const std::vector<std::pair<int32_t, size_t>>& entries,
                                       unsigned i, unsigned j);
    
    unsigned storeFASTpage(int32_t v[], unsigned offset,
                          const std::vector<std::pair<int32_t, size_t>>& entries,
                          unsigned i, unsigned j, unsigned levels);
    
    inline unsigned maskToIndex(unsigned bitmask) const;
    unsigned searchInternal(int32_t key) const;
    
    template<typename DateType>
    int32_t dateToInt32(const DateType& date) const;
    
    template<typename DateType>
    DateType int32ToDate(int32_t value) const;

public:
    FastTree();
    ~FastTree();
    
    template<typename DateType, typename ValueType>
    void build(const std::vector<Entry<DateType, ValueType>>& entries);
    
    template<typename DateType, typename ValueType>
    size_t search(const DateType& date) const;
    
    template<typename DateType, typename ValueType>
    RangeResult<DateType, ValueType> rangeLessThan(
        const DateType& cutoff,
        const std::vector<Entry<DateType, ValueType>>& original_data) const;
    
    template<typename DateType, typename ValueType>
    RangeResult<DateType, ValueType> rangeSearch(
        const DateType& start,
        const DateType& end,
        const std::vector<Entry<DateType, ValueType>>& original_data) const;
    
    size_t getTreeSize() const { return tree_size_; }
    size_t getDataSize() const { return data_size_; }
    size_t getMemoryUsage() const;
};

} 
