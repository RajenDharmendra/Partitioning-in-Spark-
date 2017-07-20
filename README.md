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



Home  /  AcadGild • Spark  /  Understanding Partitioning in Spark
Partitioning in Spark
21 December 2016
Understanding Partitioning in Spark

In this tutorial, we will discuss partitioning in Spark. Partitioning is nothing but dividing it into parts. If you talk about partitioning in distributed system, we can define it as the division of the large dataset and store them as multiple parts across the cluster.

Spark works on data locality principle. Worker nodes takes the data for processing that are nearer to them. By doing partitioning network I/O will be reduced so that data can be processed a lot faster.

In Spark, operations like co-group, groupBy, groupByKey and many more will need lots of I/O operations. In this scenario, if we apply partitioning, then we can reduce the number of I/O operations rapidly so that we can speed up the data processing.

To divide the data into partitions first we need to store it. Spark stores its data in form of RDDs.
Resilient Distributed Datasets

Resilient Distributed Datasets (RDD) is a simple and immutable distributed collection of objects. Each RDD is split into multiple partitions which may be computed on different nodes of the cluster. In Spark, every function is performed on RDDs only.

Spark revolves around the concept of a resilient distributed dataset (RDD), which is a fault-tolerant collection of elements that can be operated on in parallel.

Let’s see now the features of Resilient Distributed Datasets in the below explanation:

    In Hadoop, we store the data as blocks and store them in different data nodes. In Spark, instead of following the above approach, we make partitions of the RDDs and store in worker nodes (datanodes) which are computed in parallel across all the nodes.

    In Hadoop, we need to replicate the data for fault recovery, but in case of Spark, replication is not required as this is performed by RDDs.

    RDDs load the data for us and are resilient which means they can be recomputed.

    RDDs perform two types of operations: Transformations which creates a new dataset from the previous RDD and actions which return a value to the driver program after performing the computation on the dataset.

    RDDs keeps a track of transformations and checks them periodically. If a node fails, it can rebuild the lost RDD partition on the other nodes, in parallel.

Spark has two types of partitioning techniques. One is HashPartitioner and the other is RangePartitioner. Let us see about each of them in detail.

HashPartitioner

HashPartitioner works on Java’s Object.hashcode(). The concept of hashcode() is that objects which are equal should have the same hashcode. So based on this hashcode() concept HashPartitioner will divide the keys that have the same hashcode().

It is the default partitioner of Spark. If we did not mention any partitioner then Spark will use this hashpartitioner for partitioning the data.

RangePartitioner

If there are sortable records, then range partition will divide the records almost in equal ranges. The ranges are determined by sampling the content of the RDD passed in.

First, the RangePartitioner will sort the records based on the key and then it will divide the records into a number of partitions based on the given value.

CustomPartitioner

We can also customize the number of partitions we need and what should be stored in those partitions by extending the default Partitioner class in Spark.

Let us see the demo of all these partitioning concepts in Spark.

# HashPartitioner

We will include HashPartitioner in the word count program. Below is the code for wordcount program. Here we have used HashPartitioner to partition the (Key, Value) pairs i.e., (word,1). After this, we have performed reduceByKey() action and we have saved all the output in a directory.

    scala> val textFile = sc.textFile("file:///path/to/file_wc")
 
    textFile: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[29] at textFile at <console>:16

    scala> val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).partitionBy(new HashPartitioner(10))

    counts: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[32] at partitionBy at <console>:18

    scala> counts.reduceByKey(_+_).saveAsTextFile("/path/to/partition_spark/hash")

    scala>


Now in the specified directory, you can see 10 files with word count performed on the words that are present in the files.

<a href="http://imgur.com/VykiIOq"><img src="http://i.imgur.com/VykiIOq.png" title="source: imgur.com" /></a>


So the output of each partition is based on the words that are present in each file.

Note: If you use collect action to view the result then it will collect the output from all the partitions and send it to the master. Sometimes this might cause OutOfMemory exception also we cannot see the partition files. So it is better to save the output in a file.


# RangePartitioner

As specified earlier, rangePartitioner partitions the data based on the range and the ranges are determined by sampling the content of the RDD passed in.

Here we are performing wordcount operation and we are involving rangePartitioner to partition the key value pairs of (word,1).

Here we have given the number of partitions that rangePartitioner should create as 10. So rangePartitioner will create 10 equal size range of partitions. Here is the code to implement rangePartitioner in scala.


    scala> val textFile = sc.textFile("file:///path/to/file_wc")
 
    textFile: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[41] at textFile at <console>:17

    scala> val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1))

    counts: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[43] at map at <console>:19

    scala> val range = counts.partitionBy(new RangePartitioner(10,counts))

    range: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[46] at partitionBy at <console>:21

    scala> range.reduceByKey(_+_).saveAsTextFile("/path/to/partition_spark/range")

    scala>
    
In the below screenshot you can see that there are 10 partitions created and wordcount operation is performed on each partition individually.


<a href="http://imgur.com/FhfEpJr"><img src="http://i.imgur.com/FhfEpJr.png" title="source: imgur.com" /></a>



Above are the results of word count operation performed on 10 partitioned files in parallel. So we have got 10 output files. In these 10 files, word count operation is performed among themselves and their respective results are stored accordingly.

# Custom Partitioner

Spark allows users to create custom partitioners by extending the default Partitioner class. So that we can specify the data to be stored in each partition.

Now we will implement a custom partitioner which takes out the word AcadGild separately and stores it in another partition.

Here is the code for our custom partitioner.


    class CustomPartitioner(numParts: Int) extends Partitioner {
     override def numPartitions: Int = numParts
     override def getPartition(key: Any): Int =
     {
           if(key.toString.equals("MyTestString")){
        0
     }else{
        1
    }
    }

     override def equals(MyTestString: Any): Boolean = MyTestString match
     {
     case test: CustomPartitioner =>
     test.numPartitions == numPartitions
     case _ =>
     false
     }
    }
    

Custom partitioner mainly needs these things

numPartitions: Int, it takes the number of partitions that needs to be created.

gerPartition(key: Any): Int, this method will return the particular key to the specified partition ID which ranges from 0 to numPartitions-1 for a given key.

Equals(): is the normal java equality method used to compare two objects, this method will test your partitioner object against other objects of itself then it decides whether two of your RDDs are Partitioned in the same way or not.

In the above custom partitioner program, in the getPartition method we have given a condition that if the key is MyTestString then it should go into the 1st partition i.e., partition 0 else it should go into the 2nd partition.

Let us compile this custom partitioner class and use it in our word count program. Here is the code for wordcount program using our custom partitioner.



    scala> val textFile = sc.textFile("file:///path/to/partitioning_wc")

    textFile: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[119] at textFile at <console>:18

    scala> val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1))

    counts: org.apache.spark.rdd.RDD[(String, Int)] = MapPartitionsRDD[121] at map at <console>:20

    scala> val range = counts.partitionBy(new CustomPartitioner(2))

    range: org.apache.spark.rdd.RDD[(String, Int)] = ShuffledRDD[122] at partitionBy at <console>:23

    scala> range.reduceByKey(_+_).saveAsTextFile("/path/to/partition_spark/custom")
    
    
In the partitionBy class, you need to set your CustomPartitioner and you need to pass the number of partitions it should be created as shown below.



    val range = counts.partitionBy(new CustomPartitioner(2))
    
    
As we need only two partitions, we have passed the number of partitions as 2 to the CustomPartitioner. Depending on your requirement you need to modify your custom partitioner and the number of partitions.


Let us now check for the output in the specified folder. According to our custom partitioner, the output should be partitioned in 2 files where in the first file only MyTestString should be present. Let us check for the same. So here is our final output after partitioning.

<a href="http://imgur.com/9ENRzSz"><img src="http://i.imgur.com/9ENRzSz.png?1" title="source: imgur.com" /></a>
