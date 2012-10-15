package com.twitter.rystsov.mr.identity.streaming;

import org.apache.hadoop.mapred.TextInputFormat;

/**
 * User: Denis Rystsov
 *
 * If we use TextInputFormat as InputFormat in streaming the key will be ignored
 *   see org.apache.hadoop.streaming.PipeMapper
 * Streaming use "old" map reduce API (org.apache.hadoop.mapred)
 */
public class IgnoreKeyHackTextInputFormat extends TextInputFormat {
}
