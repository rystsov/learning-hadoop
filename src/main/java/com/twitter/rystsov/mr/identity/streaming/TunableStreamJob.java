package com.twitter.rystsov.mr.identity.streaming;

import org.apache.hadoop.streaming.StreamJob;
import org.apache.hadoop.streaming.io.InputWriter;
import org.apache.hadoop.streaming.io.OutputReader;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * User: Denis Rystsov
 *
 * Adds ability to set OutputReader by user
 */
public class TunableStreamJob extends StreamJob {
    class Rollback {
        public final String name;
        public final Class<?> xface;
        public final Class<?> value;

        Rollback(String name, Class<?> xface) {
            this.name = name;
            this.xface = xface;
            this.value = config_.getClass(name, null);
        }

        public void rollback() {
            if (value!=null) {
                jobConf_.setClass(name, value, xface);
            }
        }
    }

    @Override
    protected void setJobConf() throws IOException {
        List<Rollback> rollbacks = Arrays.<Rollback>asList(
                new Rollback("stream.map.input.writer.class", InputWriter.class),
                new Rollback("stream.reduce.input.writer.class", InputWriter.class),
                new Rollback("stream.map.output.reader.class", OutputReader.class),
                new Rollback("stream.reduce.output.reader.class", OutputReader.class),
                new Rollback("mapred.mapoutput.key.class", Object.class),
                new Rollback("mapred.mapoutput.value.class", Object.class),
                new Rollback("mapred.output.key.class", Object.class),
                new Rollback("mapred.output.value.class", Object.class)
        );

        super.setJobConf();

        for (Rollback rollback : rollbacks) rollback.rollback();
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new TunableStreamJob(), args);
    }
}
