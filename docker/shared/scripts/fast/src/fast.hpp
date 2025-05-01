#pragma once
#include <vector>
#include <cstdint>

namespace fast {

class FastIndex {
public:
    explicit FastIndex(const std::vector<int32_t>& keys, unsigned k = 3);
    ~FastIndex();

    uint32_t upper_bound(int32_t key) const;

    uint64_t search(int32_t key) const;

    std::size_t size()   const { return n_; }

private:
    struct LeafEntry { int32_t key; uint64_t value; };

    static unsigned pow16(unsigned exponent) { return 1U << (exponent << 2); }
    static unsigned median(unsigned i, unsigned j) { return i + (j - 1 - i) / 2; }
    static unsigned maskToIndex(unsigned bitmask);

    static void   storeSIMDblock(int32_t*, unsigned, LeafEntry*, unsigned, unsigned);
    static unsigned storeCachelineBlock(int32_t*, unsigned, LeafEntry*, unsigned, unsigned);
    static unsigned storeFASTpage(int32_t*, unsigned, LeafEntry*, unsigned, unsigned, unsigned);

    unsigned       K_;
    unsigned       scale_;
    unsigned       n_;
    LeafEntry*     leaves_;
    int32_t*       fast_;
};

} 
