package com.herminen.job;

import com.herminen.inputformat.WholeFileInputFormat;
import com.herminen.mapper.SequenceFileMapper;
import com.herminen.reducer.SequenceFileReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * Created on 2021/2/23.
 *
 * @author ${AUTHOR}
 */
public class SequenceFileTask {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        //跨平台执行，windows
        conf.set("mapreduce.app-submission.cross-platform","true");
        Job job = Job.getInstance(conf, "sequence_file_combiner");
        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setJarByClass(SequenceFileTask.class);
        job.setMapperClass(SequenceFileMapper.class);
        job.setCombinerClass(SequenceFileReducer.class);
        job.setReducerClass(SequenceFileReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        FileInputFormat.addInputPath(job, new Path("/herminen/testdata/sequence/input/"));
        FileOutputFormat.setOutputPath(job, new Path("/herminen/testdata/sequence/output/"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
