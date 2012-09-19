package com.twitter.rystsov.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**

 * User: Denis Rystsov
 */
public class SequenceFileSimpleRead {
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


    public static void main(String[] args) throws URISyntaxException, IOException, IllegalAccessException, InstantiationException {
        String to = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(to), conf);

        SequenceFile.Reader reader = null;

        try {
            reader = new SequenceFile.Reader(fs, new Path(to), conf);
            Writable key = (Writable)reader.getKeyClass().newInstance();
            Writable value = (Writable)reader.getValueClass().newInstance();

            System.out.println("Metadata:");
            System.out.println("\tkey: " + reader.getKeyClassName());
            System.out.println("\tvalue: " + reader.getValueClassName());
            System.out.println("\tis compressed: " + reader.isCompressed());
            if (reader.isCompressed()) {
                System.out.println("\tis block compressed: " + reader.isBlockCompressed());
                System.out.println("\tcompression: " + reader.getCompressionCodec());
            }
            System.out.println("Data:");

            while (reader.next(key, value)) {
                System.out.println("\t" + key.toString() + "\t" + value.toString());
            }
        } finally {
            // ignore null & IOExceptions
            IOUtils.closeStream(reader);
        }
    }
}
