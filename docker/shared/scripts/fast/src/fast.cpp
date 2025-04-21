#include "fast.hpp"
#include <immintrin.h>
#include <sys/mman.h>
#include <cassert>
#include <climits>
#include <cstring>

static inline void* malloc_huge(std::size_t size) {
    void* p = mmap(nullptr, size, PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
#ifdef __linux__
    madvise(p, size, MADV_HUGEPAGE);
#endif
    return p;
}

static const unsigned maskTbl[8] = {0,9,1,2,9,9,9,3};
inline unsigned fast::FastIndex::maskToIndex(unsigned bm) { return maskTbl[bm & 7]; }

fast::FastIndex::FastIndex(const std::vector<int32_t>& keys_in, unsigned k)
  : K_(k)
{
    n_ = 1U << (16 + (K_ << 2));  
    std::vector<int32_t> keys = keys_in;
    keys.push_back(INT_MAX);
    keys.resize(n_, INT_MAX);

    leaves_ = new LeafEntry[n_];
    for (unsigned i = 0; i < n_; ++i) {
        leaves_[i].key   = keys[i];
        leaves_[i].value = i;
    }

    unsigned slots = 0;
    for (unsigned i = 0; i < K_ + 4; ++i) slots += pow16(i);
    slots = slots * 64 / 4;                        
    fast_ = static_cast<int32_t*>(malloc_huge(sizeof(int32_t) * slots));

    unsigned off = storeFASTpage(fast_, 0, leaves_, 0, n_, 4);
    unsigned chunk = n_ >> 16;
    for (unsigned i = 0; i < (1U << 16); ++i)
        off = storeFASTpage(fast_, off, leaves_, i * chunk, (i + 1) * chunk, K_);

    assert(off == slots);

    scale_ = 0;
    for (unsigned i = 0; i < K_; ++i) scale_ += pow16(i);
    scale_ *= 16;
}

fast::FastIndex::~FastIndex() {
    munmap(fast_, sizeof(int32_t) * n_);   
    delete [] leaves_;
}


void fast::FastIndex::storeSIMDblock(
    int32_t v[], unsigned k,
    LeafEntry l[], unsigned i, unsigned j)
{
unsigned m = median(i, j);
v[k + 0]   = l[m].key;
v[k + 1]   = l[median(i, m)].key;
v[k + 2]   = l[median(1 + m, j)].key;
}

unsigned fast::FastIndex::storeCachelineBlock(
    int32_t v[], unsigned k,
    LeafEntry l[], unsigned i, unsigned j)
{
storeSIMDblock(v, k + 3 * 0, l, i, j);
unsigned m = median(i, j);
storeSIMDblock(v, k + 3 * 1, l, i, median(i, m));
storeSIMDblock(v, k + 3 * 2, l, median(i, m) + 1, m);
storeSIMDblock(v, k + 3 * 3, l, m + 1, median(m + 1, j));
storeSIMDblock(v, k + 3 * 4, l, median(m + 1, j) + 1, j);
return k + 16;                   
}

unsigned fast::FastIndex::storeFASTpage(
    int32_t v[], unsigned offset,
    LeafEntry l[], unsigned i, unsigned j, unsigned levels)
{
for (unsigned lvl = 0; lvl < levels; ++lvl) {
    unsigned chunk = (j - i) / pow16(lvl);
    for (unsigned cl = 0; cl < pow16(lvl); ++cl)
        offset = storeCachelineBlock(v, offset,
                                     l, i + cl * chunk, i + (cl + 1) * chunk);
}
return offset;
}

uint64_t fast::FastIndex::search(int32_t key_q) const
{
    __m128i q = _mm_set1_epi32(key_q);

    unsigned page_off = 0, level_off = 0;

    for (unsigned cl = 1; cl <= 4; ++cl) {
        __m128i tree = _mm_loadu_si128((__m128i*)
                         (fast_ + page_off + level_off * 16));
        unsigned idx  = _mm_movemask_ps(
                         _mm_castsi128_ps(_mm_cmpgt_epi32(q, tree)));
        unsigned child = maskToIndex(idx);

        tree = _mm_loadu_si128((__m128i*)
               (fast_ + page_off + level_off * 16 + 3 + 3 * child));
        idx  = _mm_movemask_ps(
               _mm_castsi128_ps(_mm_cmpgt_epi32(q, tree)));

        unsigned cache_off = child * 4 + maskToIndex(idx);
        level_off = level_off * 16 + cache_off;
        page_off += pow16(cl);
    }

    unsigned pos  = level_off;
    unsigned base = 69904 + level_off * scale_;

    page_off = level_off = 0;

    for (unsigned cl = 1; cl <= K_; ++cl) {
        __m128i tree = _mm_loadu_si128((__m128i*)
                         (fast_ + base + page_off + level_off * 16));
        unsigned idx  = _mm_movemask_ps(
                         _mm_castsi128_ps(_mm_cmpgt_epi32(q, tree)));
        unsigned child = maskToIndex(idx);

        tree = _mm_loadu_si128((__m128i*)
               (fast_ + base + page_off + level_off * 16 + 3 + 3 * child));
        idx  = _mm_movemask_ps(
               _mm_castsi128_ps(_mm_cmpgt_epi32(q, tree)));

        unsigned cache_off = child * 4 + maskToIndex(idx);
        level_off = level_off * 16 + cache_off;
        page_off += pow16(cl);
    }
    return (static_cast<uint64_t>(pos) << (K_ << 2)) | level_off;
}

