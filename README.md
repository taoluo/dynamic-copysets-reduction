# Dynamic Copysets Merging
## Introduction
Large-scale cloud storage systems rely heavily on data replication to ensure durability and availability. The most common approach, Random Replication, distributes replicas across randomly selected nodes. While effective against uncorrelated single-node failures, random replication is highly vulnerable to correlated failures, such as cluster-wide power outages, where a small percentage of nodes fail simultaneously. In large clusters, this often guarantees data loss.

Copyset Replication [1] was introduced as a technique to significantly reduce the frequency of data loss events in such scenarios. Instead of purely random placement, Copyset Replication groups nodes into sets of size R (the replication factor), called copysets. Replicas of a data chunk are constrained to be stored only within a single copyset. This drastically reduces the number of potential failure combinations that can lead to data loss (only the simultaneous failure of all nodes within a specific copyset causes loss). While this reduces the frequency of data loss incidents, it accepts that when a loss event does occur, more data might be lost compared to random replication. Copyset Replication aims to provide a controllable trade-off between data loss frequency and the amount of data lost per incident, often favoring fewer, larger loss events due to the high fixed costs associated with any data loss.

However, the original Copyset Replication and related techniques like Tiered Replication [2] primarily address static or slowly changing cluster topologies. In real-world dynamic cloud environments, nodes frequently join and leave the cluster. Applying existing copyset generation algorithms (like the greedy approach in Tiered Replication) in such dynamic settings leads to a gradual increase in the total number of active copysets over time. This increase in copysets unfortunately counteracts the primary benefit of the approach, raising the overall probability of data loss as the cluster evolves.

This project introduces a Dynamic Copysets Merging algorithm designed to address the challenge of maintaining the benefits of Copyset Replication in dynamic cluster environments. Our algorithm can be run periodically in the background to proactively merge existing copysets, reducing their total number and keeping the probability of data loss close to the theoretical optimum, even as nodes are added or removed. The merging process is designed to introduce minimal performance overhead, making Copyset Replication a more practical and robust strategy for real-world, dynamic distributed storage systems.

Please see the project report PDF for more details.

## References
[1] Cidon, A., Rumble, S. M., Stutsman, R., Katti, S., Ousterhout, J., & Rosenblum, M. (2013). Copysets: Reducing the Frequency of Data Loss in Cloud Storage. Proceedings of the 2013 USENIX Annual Technical Conference (USENIX ATC '13).

[2] Cidon, A., et al. Tiered Replication: A Cost-effective Alternative to Full Cluster Geo-replication. (Note: Provide full citation if available, the project report references this)
