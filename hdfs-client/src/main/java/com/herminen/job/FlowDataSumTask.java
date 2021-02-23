package com.herminen.job;

import com.herminen.mapper.FlowDataMapper;
import com.herminen.mapper.TokenizerMapper;
import com.herminen.reducer.FlowDataReducer;
import com.herminen.reducer.IntSumReducer;
import com.herminen.writable.FlowBeanWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created on 2021/2/23.
 *
 * @author ${AUTHOR}
 */
public class FlowDataSumTask {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        //windows平台也能调度
        conf.set("mapreduce.app-submission.cross-platform","true");
        //调度任务类
        Job job = Job.getInstance(conf, "flowDataStatisticJob");
        job.setJarByClass(FlowDataSumTask.class);
        job.setMapperClass(FlowDataMapper.class);
        job.setCombinerClass(FlowDataReducer.class);
        job.setReducerClass(FlowDataReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBeanWritable.class);
        FileInputFormat.addInputPath(job, new Path("/herminen/testdata/flowdata/input/"));
        FileOutputFormat.setOutputPath(job, new Path("/herminen/testdata/flowdata/output/"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
