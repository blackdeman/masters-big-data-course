#!/bin/bash

hadoop fs -copyFromLocal inputSmall.tsv /tmp/

pig -x local pig-small.txt

hadoop fs -getmerge /tmp/task4outputSmall/ task4outputSmall.tsv

hadoop fs -rm /tmp/inputSmall.tsv
hadoop fs -rm -r /tmp/task4outputSmall
