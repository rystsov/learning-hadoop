package com.twitter.rystsov.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

public class SimpleWrite
{
    public static void main( String[] args ) throws URISyntaxException, IOException {
        String from = args[0];
        String to =args[1];

        InputStream src = new BufferedInputStream(new FileInputStream(from));

        Configuration conf = new Configuration();
        // Default replication factor is 3, so we
        //   must change it to avoid under-replicated cluster state
        // The following inequality must be true:
        //   dfs.replication.min <= dfs.replication <= #datanodes
        conf.set("dfs.replication","1");

        FileSystem fs = FileSystem.get(new URI(to), conf);
        FSDataOutputStream dst = fs.create(new Path(to), true);

        // true => closes both src and dst
        IOUtils.copyBytes(src, dst, 4096, true);
    }
}
