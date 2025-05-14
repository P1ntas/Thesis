#pragma once

#include <cstdint>
#include <vector>
#include <string>
#include <emmintrin.h>
#include <sys/mman.h>
#include <iostream>
#include <cassert>
#include <climits>

namespace fast {

struct LeafEntry {
    int32_t key;
    uint64_t value;
};

class DateConverter {
public:
    static int32_t dateToInt(const std::string& dateStr) {
        if (dateStr.length() < 10) {
            return 0;
        }

        int year = std::stoi(dateStr.substr(0, 4));
        int month = std::stoi(dateStr.substr(5, 2));
        int day = std::stoi(dateStr.substr(8, 2));
        
        return year * 10000 + month * 100 + day;
    }

    static std::string intToDate(int32_t dateInt) {
        int day = dateInt % 100;
        dateInt /= 100;
        int month = dateInt % 100;
        int year = dateInt / 100;
        
        char buffer[11];
        snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d", year, month, day);
        return std::string(buffer);
    }
};

class FastIndex {
private:
    static const unsigned K = 3;  
    int32_t* fastTree;            
    size_t treeSize;              
    unsigned scale;               
    size_t dataSize;             

    void* malloc_huge(size_t size) {
        void* p = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    #if __linux__
        madvise(p, size, MADV_HUGEPAGE);
    #endif
        return p;
    }

    inline unsigned pow16(unsigned exponent) const {
        return 1 << (exponent << 2);
    }

    inline unsigned median(unsigned i, unsigned j) const {
        return i + (j - 1 - i) / 2;
    }

    inline void storeSIMDblock(int32_t v[], unsigned k, LeafEntry l[], unsigned i, unsigned j) {
        unsigned m = median(i, j);
        v[k+0] = l[m].key;
        v[k+1] = l[median(i, m)].key;
        v[k+2] = l[median(1+m, j)].key;
    }

    inline unsigned storeCachelineBlock(int32_t v[], unsigned k, LeafEntry l[], unsigned i, unsigned j) {
        storeSIMDblock(v, k+3*0, l, i, j);
        unsigned m = median(i, j);
        storeSIMDblock(v, k+3*1, l, i, median(i, m));
        storeSIMDblock(v, k+3*2, l, median(i, m)+1, m);
        storeSIMDblock(v, k+3*3, l, m+1, median(m+1, j));
        storeSIMDblock(v, k+3*4, l, median(m+1, j)+1, j);
        return k+16;
    }

    unsigned storeFASTpage(int32_t v[], unsigned offset, LeafEntry l[], unsigned i, unsigned j, unsigned levels) {
        for (unsigned level = 0; level < levels; level++) {
            unsigned chunk = (j - i) / pow16(level);
            for (unsigned cl = 0; cl < pow16(level); cl++)
                offset = storeCachelineBlock(v, offset, l, i + cl * chunk, i + (cl + 1) * chunk);
        }
        return offset;
    }

    int32_t* buildFAST(LeafEntry l[], unsigned len) {
        unsigned n = 0;
        for (unsigned i = 0; i < K+4; i++)
            n += pow16(i);
        n = n * 64 / 4;
        int32_t* v = (int32_t*)malloc_huge(sizeof(int32_t) * n);

        unsigned offset = storeFASTpage(v, 0, l, 0, len, 4);
        unsigned chunk = len / (1 << 16);
        for (unsigned i = 0; i < (1 << 16); i++)
            offset = storeFASTpage(v, offset, l, i * chunk, (i + 1) * chunk, K);
        assert(offset == n);

        return v;
    }

    inline unsigned maskToIndex(unsigned bitmask) const {
        static unsigned table[8] = {0, 9, 1, 2, 9, 9, 9, 3};
        return table[bitmask & 7];
    }

public:
    FastIndex(const std::vector<int32_t>& keys) {
        dataSize = keys.size();
        
        unsigned n = (1 << (16 + (K * 4))); 
        
        LeafEntry* leaves = new LeafEntry[n];
        for (unsigned i = 0; i < keys.size(); i++) {
            leaves[i].key = keys[i];
            leaves[i].value = i;
        }
        
        for (unsigned i = keys.size(); i < n; i++) {
            leaves[i].key = INT_MAX;
            leaves[i].value = i;
        }
        
        fastTree = buildFAST(leaves, n);
        treeSize = n;
        
        scale = 0;
        for (unsigned i = 0; i < K; i++)
            scale += pow16(i);
        scale *= 16;
        
        delete[] leaves;
        
        std::cout << "FAST tree built with " << dataSize << " elements, padded to " << n << std::endl;
    }

    FastIndex(const std::vector<std::string>& dateStrings) {
        std::vector<int32_t> dateInts;
        dateInts.reserve(dateStrings.size());
        
        for (const auto& dateStr : dateStrings) {
            dateInts.push_back(DateConverter::dateToInt(dateStr));
        }
        
        dataSize = dateInts.size();
        
        unsigned n = (1 << (16 + (K * 4))); 
        
        LeafEntry* leaves = new LeafEntry[n];
        for (unsigned i = 0; i < dateInts.size(); i++) {
            leaves[i].key = dateInts[i];
            leaves[i].value = i;
        }
        
        for (unsigned i = dateInts.size(); i < n; i++) {
            leaves[i].key = INT_MAX;
            leaves[i].value = i;
        }
        
        fastTree = buildFAST(leaves, n);
        treeSize = n;
        
        scale = 0;
        for (unsigned i = 0; i < K; i++)
            scale += pow16(i);
        scale *= 16;
        
        delete[] leaves;
        
        std::cout << "FAST tree built with " << dataSize << " date elements, padded to " << n << std::endl;
    }
    
    ~FastIndex() {
        if (fastTree) {
            munmap(fastTree, treeSize * sizeof(int32_t));
        }
    }

    unsigned search(int32_t key_q) const {
        __m128i xmm_key_q = _mm_set1_epi32(key_q);

        unsigned page_offset = 0;
        unsigned level_offset = 0;

        for (unsigned cl_level = 1; cl_level <= 4; cl_level++) {
            __m128i xmm_tree = _mm_loadu_si128((__m128i*)(fastTree + page_offset + level_offset * 16));
            __m128i xmm_mask = _mm_cmpgt_epi32(xmm_key_q, xmm_tree);
            unsigned index = _mm_movemask_ps(_mm_castsi128_ps(xmm_mask));
            unsigned child_index = maskToIndex(index);

            xmm_tree = _mm_loadu_si128((__m128i*)(fastTree + page_offset + level_offset * 16 + 3 + 3 * child_index));
            xmm_mask = _mm_cmpgt_epi32(xmm_key_q, xmm_tree);
            index = _mm_movemask_ps(_mm_castsi128_ps(xmm_mask));

            unsigned cache_offset = child_index * 4 + maskToIndex(index);
            level_offset = level_offset * 16 + cache_offset;
            page_offset += pow16(cl_level);
        }

        unsigned pos = level_offset;
        unsigned offset = 69904 + level_offset * scale;
        page_offset = 0;
        level_offset = 0;

        for (unsigned cl_level = 1; cl_level <= K; cl_level++) {
            __m128i xmm_tree = _mm_loadu_si128((__m128i*)(fastTree + offset + page_offset + level_offset * 16));
            __m128i xmm_mask = _mm_cmpgt_epi32(xmm_key_q, xmm_tree);
            unsigned index = _mm_movemask_ps(_mm_castsi128_ps(xmm_mask));
            unsigned child_index = maskToIndex(index);

            xmm_tree = _mm_loadu_si128((__m128i*)(fastTree + offset + page_offset + level_offset * 16 + 3 + 3 * child_index));
            xmm_mask = _mm_cmpgt_epi32(xmm_key_q, xmm_tree);
            index = _mm_movemask_ps(_mm_castsi128_ps(xmm_mask));

            unsigned cache_offset = child_index * 4 + maskToIndex(index);
            level_offset = level_offset * 16 + cache_offset;
            page_offset += pow16(cl_level);
        }

        return (pos << (K * 4)) | level_offset;
    }

    unsigned searchDate(const std::string& dateStr) const {
        int32_t dateInt = DateConverter::dateToInt(dateStr);
        return search(dateInt);
    }

    size_t size() const {
        return treeSize;
    }

    size_t originalSize() const {
        return dataSize;
    }
};

}