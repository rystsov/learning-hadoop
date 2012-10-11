package com.twitter.rystsov.mr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * User: Denis Rystsov
 */
public class MultithreadedWordCount {

    // class should be thread save
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        public static enum PREPOST { SETUP, CLEANUP }

        @Override()
        protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws java.io.IOException, java.lang.InterruptedException {
            // will be called several times
            context.getCounter(PREPOST.SETUP).increment(1);
        }

        @Override
        protected void map(LongWritable key, Text value,
                     Context context) throws IOException, InterruptedException {

            String[] words = value.toString().toLowerCase().split("[\\p{Blank}[\\p{Punct}]]+");
            for (String word : words) {
                context.write(new Text(word), new LongWritable(1));
            }
        }

        @Override()
        protected void cleanup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws java.io.IOException, InterruptedException {
            // will be called several times
            context.getCounter(PREPOST.CLEANUP).increment(1);
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

        MultithreadedMapper.setMapperClass(job, MultithreadedWordCount.WordCountMapper.class);
        MultithreadedMapper.setNumberOfThreads(job, 10);

        job.setMapperClass(MultithreadedMapper.class);
        job.setCombinerClass(MultithreadedWordCount.WordCountReducer.class);
        job.setReducerClass(MultithreadedWordCount.WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        /* begin defaults */
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        /* end defaults */

        job.waitForCompletion(true);
    }
}
