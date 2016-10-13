package edu.gatech.cse6242;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Task1 {

    public static class GraphEdgesMapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        private IntWritable vertex = new IntWritable();
        private IntWritable weight = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] lineParts = value.toString().split("\t");

            vertex.set(Integer.parseInt(lineParts[1]));
            weight.set(Integer.parseInt(lineParts[2]));

            context.write(vertex, weight);
        }
    }

    public static class EdgeWeightReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(Task1.class);
        job.setMapperClass(GraphEdgesMapper.class);
        job.setCombinerClass(EdgeWeightReducer.class);
        job.setReducerClass(EdgeWeightReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
