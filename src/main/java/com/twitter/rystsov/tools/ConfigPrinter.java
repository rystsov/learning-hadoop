package com.twitter.rystsov.tools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Map;

/**
 * User: Denis Rystsov
 */
public class ConfigPrinter extends Configured implements Tool {
    static {
        /*
            From a doc (http://hadoop.apache.org/docs/current/api/org/apache/hadoop/conf/Configuration.html):

            Unless explicitly turned off, Hadoop by default specifies two resources, loaded in-order from the classpath:
            core-default.xml : Read-only defaults for hadoop.
            core-site.xml: Site-specific configuration for a given hadoop installation.

            NB:
              When I put core-default.xml on classpath the app falls
              Hadoop version 1.0.3
        */

        // load hdfs-site.xml from classpath
        // Configuration.addDefaultResource("hdfs-site.xml");
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration config =  this.getConf();
        for (Map.Entry<String, String> entry : config) {
            System.out.println(entry.getKey() + " = " + entry.getValue());
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new ConfigPrinter(), args);
    }
}
