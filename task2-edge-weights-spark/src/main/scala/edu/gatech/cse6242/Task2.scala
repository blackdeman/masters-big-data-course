package edu.gatech.cse6242

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Task2 {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Task2"))

    // read the file
    val file = sc.textFile("hdfs://localhost:8020" + args(0))

    val output = file
      .map(line => (line.split("\t")(1), line.split("\t")(2).toInt))
        .reduceByKey((x, y) => x + y)
          .map(tuple => tuple.productIterator.mkString("\t"))

    output.saveAsTextFile("hdfs://localhost:8020" + args(1))
  }
}
