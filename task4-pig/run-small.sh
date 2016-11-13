#!/bin/bash

hadoop fs -copyFromLocal inputSmall.tsv /tmp/

pig -x mapreduce pig-small.txt

hadoop fs -getmerge /tmp/task4outputSmall/ task4outputSmall.tsv

hadoop fs -rm /tmp/input-small.tsv
hadoop fs -rm -r /tmp/task4outputSmall
