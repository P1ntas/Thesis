#pragma once
#include "kdtree.hpp"
#include <cstdint>
#include <vector>

namespace vec {

template <std::size_t Dim = 3>
struct Entry {
    std::array<float, Dim> key{};  
    std::uint64_t          value{};
};

template <std::size_t Dim = 3>
class vKdTree
{
public:
    using entry_type = Entry<Dim>;

    void build(const std::vector<entry_type>& entries)
    {
        std::vector<std::array<float, Dim>> pts;
        pts.reserve(entries.size());
        m_values.reserve(entries.size());

        for (auto const& e : entries) {
            pts.push_back(e.key);
            m_values.push_back(e.value);
        }
        m_tree.build(pts);
    }

    struct Result { std::vector<entry_type> entries; };

    template <typename... Floats>
    Result rangeSearch(Floats... bounds) const
    {
        static_assert(sizeof...(bounds) == Dim*2,
                      "need exactly 2*Dim bounds");
        std::array<float, Dim*2> b{static_cast<float>(bounds)...};
        std::array<float, Dim> lo{}, hi{};
        for (std::size_t i = 0; i < Dim; ++i) {
            lo[i] = b[i];
            hi[i] = b[i+Dim];
        }

        auto idxs = m_tree.range_query(lo, hi);
        Result r; 
        r.entries.reserve(idxs.size());
        for (auto id : idxs) {
            entry_type e;
            e.key   = m_tree.point(id);
            e.value = m_values[id];
            r.entries.push_back(e);
        }
        return r;
    }

    std::size_t getMemoryUsage() const noexcept
    {
        return m_tree.memoryUsage()
             + m_values.capacity() * sizeof(std::uint64_t);
    }

private:
    skd::KdTree<Dim, float>       m_tree;
    std::vector<std::uint64_t>    m_values;
};

using TripleEntry  = Entry<3>;
using TripleKdTree = vKdTree<3>;

struct DateEntry {
    int32_t  key{};
    uint64_t value{};

    int32_t&       operator[](std::size_t)       { return key; }
    int32_t const& operator[](std::size_t) const { return key; }
};

} 

namespace std {
template <> struct tuple_size<vec::DateEntry> : std::integral_constant<std::size_t, 1> {};
template <> struct tuple_element<0, vec::DateEntry> { using type = int32_t; };
}
