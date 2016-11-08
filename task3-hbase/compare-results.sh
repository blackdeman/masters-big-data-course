#!/bin/bash

echo "Print diff for results for graph1"
diff <(sort ../task2-edge-weights-spark/task2output1.tsv) <(sort spark-with-hbase/task3output1.tsv)

echo "Print diff for results for graph2"
diff <(sort ../task2-edge-weights-spark/task2output2.tsv) <(sort spark-with-hbase/task3output2.tsv)
