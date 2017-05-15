# PHUI-Miner

*PHUI-Miner* parallelizes the state-of-the-art high utility itemset mining algorithm *HUI-Miner*. In *PHUI-Miner*, the search space of the high utility itemset mining problem is divided and assigned to nodes in a cluster, which splits the workload. However, *PHUI-Miner* can also be used on top of other algorithms, e.g. *FHM*. In this implementation, *FHM* is used to for better performance. 

Please cite *PHUI-Miner* as follows:

Y. Chen and A. An, [Approximate Parallel High Utility Itemset Mining](http://www.sciencedirect.com/science/article/pii/S2214579616300089), *Big Data Research*, vol. 6, Dec. 2016, pp. 26-42.

*Note: PUPGrowth in the code was the original name we used for PHUI-Miner.*
