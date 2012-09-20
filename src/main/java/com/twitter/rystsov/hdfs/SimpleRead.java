package com.twitter.rystsov.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class SimpleRead
{
    public static void main( String[] args ) throws URISyntaxException, IOException {
        String from = args[0]; /* src (hdfs file) something like hdfs://127.0.0.1:8020/home/rystsov/hi */
        String to =args[1];    /* dst (local path) like /home/rystsov/hi */

        OutputStream dst = new BufferedOutputStream(new FileOutputStream(to, /*append*/ false));


        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI(from), conf);
        FSDataInputStream src = fs.open(new Path(from));

        IOUtils.copyBytes(src, dst, 4096, /* closes both streams */true);
    }
}
