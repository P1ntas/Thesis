select
	s_name,
	s_address
from
	supplier_renamed,
	nation_renamed
where
	s_suppkey in (
		select
			ps_suppkey
		from
			partsupp_renamed
		where
			ps_partkey in (
				select
					p_partkey
				from
					part_renamed
				where
					p_name like 'forest%'
			)
			and ps_availqty > (
				select
					0.5 * sum(l_quantity)
				from
					lineitem_renamed
				where
					l_partkey = ps_partkey
					and l_suppkey = ps_suppkey
					and l_shipdate >= '1994-01-01'
					and l_shipdate < '1995-01-01'
			)
	)
	and s_nationkey = n_nationkey
	and n_name = 'CANADA'
order by
	s_name;
