package com.twitter.rystsov.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**

 * User: Denis Rystsov
 */
public class SequenceFileSimpleWrite {
    static String[] DATA = {
            "I must not fear.",
            "Fear is the mind-killer.",
            "Fear is the little-death that brings total obliteration.",
            "I will face my fear.",
            "I will permit it to pass over me and through me.",
            "And when it has gone past I will turn the inner eye to see its path.",
            "Where the fear has gone there will be nothing.",
            "Only I will remain.",
    };


    public static void main(String[] args) throws URISyntaxException, IOException {
        String to = args[0];
        Configuration conf = new Configuration();
        // turn off compression, also can be turned off via constructor
        conf.set("io.seqfile.compression.type", "NONE");
        conf.set("dfs.replication","1");
        FileSystem fs = FileSystem.get(URI.create(to), conf);

        IntWritable key = new IntWritable();
        Text value = new Text();

        FSDataOutputStream out = null;
        SequenceFile.Writer writer = null;
        try {
            // create stream explicit in order to control override flag
            out = fs.create(new Path(to), /* override */ false);
            writer = SequenceFile.createWriter(
                    conf, out, key.getClass(), value.getClass(),
                    SequenceFile.getCompressionType(conf), null, new SequenceFile.Metadata()
            );

            for (int i=0;i<DATA.length;i++) {
                key.set(DATA[i].length());
                value.set(DATA[i]);
                writer.append(key, value);
            }
        } finally {
            // ignore null & IOExceptions
            IOUtils.closeStream(writer);
            IOUtils.closeStream(out);
        }
    }
}
