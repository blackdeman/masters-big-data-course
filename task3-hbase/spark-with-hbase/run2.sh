#!/usr/bin/env bash

spark-submit --class edu.gatech.cse6242.Task3 --master local target/task3-1.0-jar-with-dependencies.jar demchenko-graph2 /user/cse6242/task3output2

hadoop fs -getmerge /user/cse6242/task3output2 task3output2.tsv
hadoop fs -rm -r /user/cse6242/task3output2
