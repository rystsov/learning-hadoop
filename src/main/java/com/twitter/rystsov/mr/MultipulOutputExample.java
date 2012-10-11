package com.twitter.rystsov.mr;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * User: Denis Rystsov
 */
public class MultipulOutputExample extends Configured implements Tool {
    public static class MultipleMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        Pattern pattern = null;
        // is it thread-safe?
        MultipleOutputs multipleOutputs;

        @Override()
        protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws java.io.IOException, java.lang.InterruptedException {
            pattern = Pattern.compile("^http://([^/]+).+$");
            multipleOutputs = new MultipleOutputs(context);
        }

        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws java.io.IOException, java.lang.InterruptedException {
            Matcher matcher = pattern.matcher(value.toString());
            if (matcher.find()) {
                context.write(new Text(matcher.group(1)), new LongWritable(1));
            } else {
                multipleOutputs.write("bad", new LongWritable(1), value);
            }
        }

        @Override()
        protected void cleanup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws java.io.IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    public static class Summer extends Reducer<Text, LongWritable, Text, LongWritable> {
        protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws java.io.IOException, java.lang.InterruptedException {
            long sum = 0;
            for(LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(MultipulOutputExample.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MultipulOutputExample.MultipleMapper.class);
        job.setReducerClass(MultipulOutputExample.Summer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        /* begin defaults */
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        /* end defaults */

        MultipleOutputs.addNamedOutput(job, "bad", TextOutputFormat.class, LongWritable.class, Text.class);

        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception, ClassNotFoundException, InterruptedException {
        ToolRunner.run(new MultipulOutputExample(), args);
    }
}
