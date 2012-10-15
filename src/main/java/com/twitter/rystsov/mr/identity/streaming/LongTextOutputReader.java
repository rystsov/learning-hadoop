package com.twitter.rystsov.mr.identity.streaming;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.streaming.PipeMapRed;
import org.apache.hadoop.streaming.io.OutputReader;
import org.apache.hadoop.streaming.io.TextOutputReader;

import java.io.IOException;

/**
 * User: Denis Rystsov
 *
 * OutputReader reads streaming process output and forms the key-value pair
 * It is responsible for kv typing
 */
public class LongTextOutputReader extends OutputReader<LongWritable, Text> {
    TextOutputReader core = new TextOutputReader();

    @Override
    public void initialize(PipeMapRed pipeMapRed) throws IOException {
        core.initialize(pipeMapRed);
    }

    @Override
    public boolean readKeyValue() throws IOException {
        return core.readKeyValue();
    }

    @Override
    public LongWritable getCurrentKey() throws IOException {
        LongWritable key = new LongWritable();
        key.set(Long.parseLong(core.getCurrentKey().toString()));
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException {
        return core.getCurrentValue();
    }

    @Override
    public String getLastOutput() {
        return core.getLastOutput();
    }
}
