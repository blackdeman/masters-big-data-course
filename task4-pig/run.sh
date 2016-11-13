#!/bin/bash

#hadoop fs -copyFromLocal googlebooks-eng-all-2gram-20120701-de.gz /tmp/

pig -x mapreduce pig-small.txt

hadoop fs -getmerge /tmp/task4output/ task4output.tsv

#hadoop fs -rm /tmp/googlebooks-eng-all-2gram-20120701-de.gz
hadoop fs -rm -r /tmp/task4output
