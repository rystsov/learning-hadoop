package com.twitter.rystsov.mr.lib.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * User: Denis Rystsov
 */
public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {
    Text name;
    BytesWritable value;
    boolean isAvailable = true;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        Configuration job = context.getConfiguration();
        final Path file = split.getPath();
        CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        if (split.getStart() != 0) throw new RuntimeException();

        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());

        ByteArrayOutputStream value = new ByteArrayOutputStream((int)split.getLength());

        if (codec != null) {
            CompressionInputStream zipIn = codec.createInputStream(fileIn);
            IOUtils.copyBytes(zipIn, value, 4096, false);
            IOUtils.closeStream(zipIn);
        } else {
            IOUtils.copyBytes(fileIn, value, 4096, false);
        }
        IOUtils.closeStream(fileIn);

        this.name = new Text(file.getName());
        this.value = new BytesWritable(value.toByteArray());
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (isAvailable) {
            isAvailable = false;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return name;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException { }
}
