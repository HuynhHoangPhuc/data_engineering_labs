| experiment | variant | seconds | total tasks | task median ms* | task max ms* | plan (final, after AQE) | note |
|---|---|---|---|---|---|---|---|
| E1 | AQE off, shuffle.partitions=200 | 1.29 | 204 | 4.0 | 28.0 | Exchange |  |
| E1 | AQE off, shuffle.partitions=8 | 0.2 | 12 | 11.0 | 15.0 | Exchange |  |
| E1 | AQE off, shuffle.partitions=1 | 0.23 | 5 | 144.0 | 146.0 | Exchange |  |
| E1 | AQE on,  shuffle.partitions=200 | 0.21 | 5 | 98.0 | 105.0 | AQEShuffleRead Exchange | AQE coalesces small shuffle partitions |
| E2 | AQE off, broadcast (threshold 10MB) | 0.6 | 13 | 31.0 | 42.0 | BroadcastExchange BroadcastHashJoin Exchange |  |
| E2 | AQE off, sort-merge (threshold -1) | 0.97 | 21 | 230.0 | 458.0 | Exchange SortMergeJoin |  |
| E2 | AQE on,  threshold -1 + broadcast() hint | 0.24 | 6 | 111.0 | 120.0 | AQEShuffleRead BroadcastExchange BroadcastHashJoin Exchange | an explicit hint beats the threshold |
| E3 | skewed SMJ, AQE off | 2.45 | 40 | 110.0 | 1263.0 | Exchange SortMergeJoin |  |
| E3 | salted SMJ (salt=64), AQE off | 2.97 | 56 | 400.0 | 935.0 | Exchange SortMergeJoin |  |
| E3 | skewed SMJ, AQE skewJoin on | 1.73 | 43 | 113.0 | 477.0 | AQEShuffleRead Exchange SortMergeJoin(skew=true) | look for AQEShuffleRead (skewed) in the SQL tab |
| E4 | repartition(64) | 5.59 | 68 | 104.0 | 917.0 | Exchange | 64 files |
| E4 | repartition(4) | 3.26 | 8 | 1847.0 | 1853.0 | Exchange | 4 files |
| E4 | coalesce(4) | 1.85 | 4 | 1717.0 | 1764.0 | Coalesce | 3 files |
| E4 | coalesce(1) | 3.58 | 1 | 3534.0 | 3534.0 | Coalesce | 1 files |
| E5 | no cache | 4.11 |  |  |  |  | 3 aggregations |
| E5 | cache materialisation (count) | 2.34 |  |  |  |  | 2869714 rows |
| E5 | with cache (MEMORY_AND_DISK) | 0.7 |  |  |  |  | 3 aggregations |
* median / max task run time of the most skewed stage of that query
