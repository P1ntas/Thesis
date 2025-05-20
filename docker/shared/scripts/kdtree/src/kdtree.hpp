#pragma once
#include <array>
#include <vector>
#include <algorithm>
#include <limits>
#include <cstddef>

namespace skd {

template <std::size_t Dim,
          typename      Scalar    = float,
          std::size_t   BucketSz  = 32>
class KdTree
{
public:
    using point_type = std::array<Scalar, Dim>;
    using index_type = std::size_t;

    KdTree()                                    = default;
    explicit KdTree(const std::vector<point_type>& pts) { build(pts); }

    void build(const std::vector<point_type>& pts)
    {
        m_points = pts;
        m_nodes.clear();
        if (!m_points.empty()) {
            m_nodes.reserve(m_points.size());
            build_rec(0, m_points.size(), 0);
        }
    }

    std::vector<index_type> range_query(const point_type& lo,
                                        const point_type& hi) const
    {
        std::vector<index_type> res;
        if (!m_nodes.empty()) range_query_impl(0, lo, hi, res);
        return res;
    }

    const point_type& point(index_type i) const noexcept { return m_points[i]; }

    std::size_t memoryUsage() const noexcept
    {
        return sizeof(*this)
             + m_points.capacity() * sizeof(point_type)
             + m_nodes.capacity()  * sizeof(Node);
    }

private:
    struct Node {
        index_type left  = npos;         
        index_type right = npos;
        index_type begin{}, end{};       
        Scalar     split{};
        std::size_t axis{};
    };
    static constexpr index_type npos = std::numeric_limits<index_type>::max();

    std::vector<Node>        m_nodes;
    std::vector<point_type>  m_points;

    index_type build_rec(index_type b, index_type e, std::size_t depth)
    {
        const index_type id = m_nodes.size();
        m_nodes.push_back({});
        Node& n   = m_nodes.back();
        n.begin   = b;  n.end = e;
        n.axis    = depth % Dim;

        if (e - b <= BucketSz) return id;             

        index_type mid = (b + e) / 2;
        std::nth_element(m_points.begin()+b, m_points.begin()+mid,
                         m_points.begin()+e,
                         [&](const point_type& a, const point_type& c)
                         { return a[n.axis] < c[n.axis]; });

        n.split = m_points[mid][n.axis];
        n.left  = build_rec(b  , mid, depth+1);
        n.right = build_rec(mid, e  , depth+1);
        return id;
    }

    void range_query_impl(index_type node,
                          const point_type& lo,
                          const point_type& hi,
                          std::vector<index_type>& out) const
    {
        const Node& n = m_nodes[node];

        if (n.left == npos)
        {
            for (index_type i = n.begin; i < n.end; ++i) {
                bool inside = true;
                for (std::size_t d = 0; d < Dim; ++d)
                    if (m_points[i][d] < lo[d] || m_points[i][d] > hi[d]) {
                        inside = false; break;
                    }
                if (inside) out.push_back(i);
            }
            return;
        }

        if (lo[n.axis] <= n.split) range_query_impl(n.left , lo, hi, out);
        if (hi[n.axis] >= n.split) range_query_impl(n.right, lo, hi, out);
    }
};

}
