package edu.gatech.cse6242

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Task3 {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Task3"))

    val conf: Configuration = new Configuration
    val arguments = new GenericOptionsParser(conf, args).getRemainingArgs

    conf.set(TableInputFormat.INPUT_TABLE, args(0))

    HBaseConfiguration.addHbaseResources(conf)

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])


    val output =
      hBaseRDD
        .flatMapValues(r => r.listCells().toArray())
          .map(p => (Bytes.toStringBinary(p._1.get()), Bytes.toStringBinary(CellUtil.cloneValue(p._2.asInstanceOf[Cell])).toInt))
            .reduceByKey((x, y) => x + y)
              .map(tuple => "%s\t%s".format(tuple._1, tuple._2.toString))

    output.saveAsTextFile("hdfs://localhost:8020" + args(1))
  }
}
