package com.herminen.job;

import com.herminen.mapper.TokenizerMapper;
import com.herminen.reducer.IntSumReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created on 2021/2/22.
 *
 * @author ${AUTHOR}
 */
public class WordCountTest {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://base1:9000");
//        conf.set("mapreduce.app-submission.cross-platform","true");
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCountTest.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/herminen/testdata/wordcount/input/"));
        FileOutputFormat.setOutputPath(job, new Path("/herminen/testdata/wordcount/output/"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
