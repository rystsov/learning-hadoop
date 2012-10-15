HADOOP_CLASSPATH=/usr/share/hadoop/contrib/streaming/hadoop-streaming-1.0.3.jar \
hadoop jar target/learning-hadoop-1.0-SNAPSHOT.jar com.twitter.rystsov.mr.identity.streaming.TunableStreamJob \
  -D stream.map.output.reader.class=com.twitter.rystsov.mr.identity.streaming.LongTextOutputReader \
  -D stream.reduce.output.reader.class=com.twitter.rystsov.mr.identity.streaming.LongTextOutputReader \
  -D mapred.mapoutput.key.class=org.apache.hadoop.io.LongWritable \
  -D mapred.mapoutput.value.class=org.apache.hadoop.io.Text \
  -D mapred.output.key.class=org.apache.hadoop.io.LongWritable \
  -D mapred.output.value.class=org.apache.hadoop.io.Text \
  -file target/learning-hadoop-1.0-SNAPSHOT.jar \
  -input /home/rystsov/stat.txt \
  -output /home/rystsov/stat.seq \
  -mapper "cat" -reducer "cat" \
  -numReduceTasks 1 \
  -inputformat com.twitter.rystsov.mr.identity.streaming.IgnoreKeyHackTextInputFormat \
  -outputformat org.apache.hadoop.mapred.SequenceFileOutputFormat