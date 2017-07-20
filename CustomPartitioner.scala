package com.spark.examples
//https://github.com/Re1tReddy/Spark/tree/master/Spark-1.5/src/main/scala/com/spark/examples
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.Partitioner


object CustomPartitioner  {
   def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Spark-Custom-Partitioner").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val inputFile = sc.textFile("file:/path/to/file/partitioner.txt")
    // data in the file
//        Mike
//        10000
//        Steve
//        2000
//        John
//        3000....
//        
    //create paired RDD 
    val pairedData = inputFile.flatMap(x => x.split(" ")).map(x => (x, 1))
    pairedData.saveAsTextFile("C:/Users/Dharmendra/Documents/Re1tReddy/pairedData.txt")
    // The above code will create tuples like (Mike,1),(10000,1),(Steve,1),(2000,1),....

    //Define custom pertitioner for paired RDD
    val partitionedData = pairedData.partitionBy(new MyCustomerPartitioner(2)).map(f => f._1)
     // we do 2 partitions of type MyCustomerPartitioner
    // map(f => f._1) emits first component of tuple
    //verify result using mapPartitionWithIndex
    val finalOut = partitionedData.mapPartitionsWithIndex {
      (partitionIndex, dataIterator) => dataIterator.map(dataInfo => (dataInfo + " is located in  " + partitionIndex + " partition."))
    }
    //Save Output in HDFS
    finalOut.saveAsTextFile("file:/path/to/file/partitionOutput")

  }
}
class MyCustomerPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts
  override def getPartition(key: Any): Int =
    {
      val out = toInt(key.toString)
      out
      // here we define what we need as out in this case integer will be out
    }
  
//Custom partitioner mainly needs these things
//
//numPartitions: Int, it takes the number of partitions that needs to be created.
//
//gerPartition(key: Any): Int, this method will return the particular key to the specified partition ID which ranges from 0 to numPartitions-1 for a given key.
//
//Equals(): is the normal java equality method used to compare two objects, this method will test your partitioner object against other objects of itself then it decides whether two of your RDDs are Partitioned in the same way or not.
//
//In the above custom partitioner program, in the getPartition method we have given a condition that if the key is int then it should go into the 1st partition i.e., partition 0 else it should go into the 2nd partition.
  
  
  override def equals(other: Any): Boolean = other match {
    case dnp: MyCustomerPartitioner =>
      dnp.numPartitions == numPartitions
    case _ =>
      false
  }

  def toInt(s: String): Int =
    {
      try {
        s.toInt
        0
      } catch {
        case e: Exception => 1
//In the above custom partitioner program, in the getPartition method we have given a condition that if the key is int then it should go into the 1st partition i.e., partition 0 else it should go into the 2nd partition.
  
      }
}
  
}
