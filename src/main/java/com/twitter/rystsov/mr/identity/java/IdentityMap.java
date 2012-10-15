package com.twitter.rystsov.mr.identity.java;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * User: Denis Rystsov
 */
public class IdentityMap extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(IdentityMap.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(1);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception, InterruptedException {
        ToolRunner.run(new IdentityMap(), args);
    }
}
