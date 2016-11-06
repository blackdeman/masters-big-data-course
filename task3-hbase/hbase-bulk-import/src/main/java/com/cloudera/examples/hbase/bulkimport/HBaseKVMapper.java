package com.cloudera.examples.hbase.bulkimport;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * HBase bulk import example
 */
public class HBaseKVMapper extends
        Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

    final static byte[] COL_FAM = "cf".getBytes();
    final static int NUM_FIELDS = 3;

    ImmutableBytesWritable hKey = new ImmutableBytesWritable();
    KeyValue kv;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] fields;

        try {
            fields = value.toString().split("\t");
        } catch (Exception ex) {
            context.getCounter("HBaseKVMapper", "PARSE_ERRORS").increment(1);
            return;
        }

        if (fields.length != NUM_FIELDS) {
            context.getCounter("HBaseKVMapper", "INVALID_FIELD_LEN").increment(1);
            return;
        }

        hKey.set(fields[1].getBytes());

        kv = new KeyValue(hKey.get(), COL_FAM, fields[0].getBytes(), fields[2].getBytes());
        context.write(hKey, kv);
    }
}
