package com.twitter.rystsov.mr;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * User: Denis Rystsov
 */
public class MultipulInputExample extends Configured implements Tool {
    public static class StatFilter extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value,
                     Context context) throws IOException, InterruptedException {

            if (value.toString().contains("stat")) {
                context.write(value, new LongWritable(1));
            }
        }
    }

    public static class Summer extends Reducer<org.apache.hadoop.io.Text, LongWritable, org.apache.hadoop.io.Text, LongWritable> {
        @Override
        protected void reduce(org.apache.hadoop.io.Text key, Iterable<LongWritable> values, Context context
                        ) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value: values) {
              sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Map<String, Class<? extends InputFormat>> formats = new HashMap<String, Class<? extends InputFormat>>();
        formats.put("seq", SequenceFileInputFormat.class);
        formats.put("txt", TextInputFormat.class);


        Job job = new Job(getConf());
        job.setJarByClass(MultipulInputExample.class);

        for (int i=0;i<args.length-1;i++) {
            String formatLabel = args[i].substring(0,3);
            MultipleInputs.addInputPath(job, new Path(args[i].substring(4)), formats.get(formatLabel));
        }
        FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(MultipulInputExample.StatFilter.class);
        job.setCombinerClass(MultipulInputExample.Summer.class);
        job.setReducerClass(MultipulInputExample.Summer.class);

        job.setOutputKeyClass(org.apache.hadoop.io.Text.class);
        job.setOutputValueClass(LongWritable.class);


        /* end defaults */

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MultipulInputExample(), args);
    }
}
