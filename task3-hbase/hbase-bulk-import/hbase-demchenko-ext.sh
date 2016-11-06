#!/bin/bash

filename_prefix=$1

echo "--- Copy from local ---"
hadoop fs -copyFromLocal $filename_prefix.tsv /tmp

echo "--- Create table ---"
echo "create 'demchenko-$filename_prefix', {NAME => 'cf'}" | hbase shell -n

echo "--- Create hfile ---"
hadoop jar target/hbase-examples-0.0.1-SNAPSHOT-job.jar com.cloudera.examples.hbase.bulkimport.Driver /tmp/$filename_prefix.tsv /tmp/$filename_prefix-hfile demchenko-$filename_prefix

echo "--- Give permissions ---"
sudo -u hdfs hadoop fs -chown -R hbase:hbase /tmp/$filename_prefix-hfile
sudo -u hdfs hadoop fs -chmod u+rw /tmp/$filename_prefix-hfile

echo "--- Bulk load ---"
hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles /tmp/$filename_prefix-hfile demchenko-$filename_prefix

echo "--- Count ---"
echo "count 'demchenko-$filename_prefix'" | hbase shell -n

echo "--- Remove from tmp ---"
hadoop fs -rm -r /tmp/$filename_prefix-hfile
hadoop fs -rm /tmp/$filename_prefix.tsv

#echo "disable 'demchenko-$filename_prefix'" | hbase shell -n
#echo "drop 'demchenko-$filename_prefix'" | hbase shell -n

