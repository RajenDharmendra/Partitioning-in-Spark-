# Partitioning-in-Spark-
Understanding Partitioning in Spark 


In this tutorial, we will discuss partitioning in Spark. Partitioning is nothing but dividing it into parts. If you talk about partitioning in distributed system, we can define it as the division of the large dataset and store them as multiple parts across the cluster.

Spark works on data locality principle. Worker nodes takes the data for processing that are nearer to them. By doing partitioning network I/O will be reduced so that data can be processed a lot faster.

In Spark, operations like co-group, groupBy, groupByKey and many more will need lots of I/O operations. In this scenario, if we apply partitioning, then we can reduce the number of I/O operations rapidly so that we can speed up the data processing.

To divide the data into partitions first we need to store it. Spark stores its data in form of RDDs.

Resilient Distributed Datasets

Resilient Distributed Datasets (RDD) is a simple and immutable distributed collection of objects. Each RDD is split into multiple partitions which may be computed on different nodes of the cluster. In Spark, every function is performed on RDDs only.

Spark revolves around the concept of a resilient distributed dataset (RDD), which is a fault-tolerant collection of elements that can be operated on in parallel.

Let’s see now the features of Resilient Distributed Datasets in the below explanation:

* In Hadoop, we store the data as blocks and store them in different data nodes. In Spark, instead of following the above approach, we make partitions of the RDDs and store in worker nodes (datanodes) which are computed in parallel across all the nodes.
* In Hadoop, we need to replicate the data for fault recovery, but in case of Spark, replication is not required as this is performed by RDDs.

* RDDs load the data for us and are resilient which means they can be recomputed.

* RDDs perform two types of operations: Transformations which creates a new dataset from the previous RDD and actions which return a value to the driver program after performing the computation on the dataset.

* RDDs keeps a track of transformations and checks them periodically. If a node fails, it can rebuild the lost RDD partition on the other nodes, in parallel.

Spark has three types of partitioning techniques. One is HashPartitioner and the other is RangePartitioner and the third one is CustomPartitioner. Let us see about each of them in detail.

# HashPartitioner

HashPartitioner works on Java’s Object.hashcode(). The concept of hashcode() is that objects which are equal should have the same hashcode. So based on this hashcode() concept HashPartitioner will divide the keys that have the same hashcode().

It is the default partitioner of Spark. If we did not mention any partitioner then Spark will use this hashpartitioner for partitioning the data.

# RangePartitioner

If there are sortable records, then range partition will divide the records almost in equal ranges. The ranges are determined by sampling the content of the RDD passed in.

First, the RangePartitioner will sort the records based on the key and then it will divide the records into a number of partitions based on the given value.

# CustomPartitioner

We can also customize the number of partitions we need and what should be stored in those partitions by extending the default Partitioner class in Spark.

Let us see the demo of all these partitioning concepts in Spark.
