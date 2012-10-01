package com.twitter.rystsov.mr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * User: Denis Rystsov
 * How to run:
 *   hadoop jar target/learning-hadoop-1.0-SNAPSHOT.jar com.twitter.rystsov.mr.WordCount src dst
 */
public class WordCount {
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value,
                     Context context) throws IOException, InterruptedException {

            String[] words = value.toString().toLowerCase().split("[\\p{Blank}[\\p{Punct}]]+");
            for (String word : words) {
                context.write(new Text(word), new LongWritable(1));
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context
                        ) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value: values) {
              sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new Job();
        job.setJarByClass(WordCount.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(WordCount.WordCountMapper.class);
        job.setCombinerClass(WordCount.WordCountReducer.class);
        job.setReducerClass(WordCount.WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        /* begin defaults */
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        /* end defaults */

        job.waitForCompletion(true);
    }
}
