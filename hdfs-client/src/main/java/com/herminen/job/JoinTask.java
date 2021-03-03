package com.herminen.job;

import com.herminen.mapper.FilterMapper;
import com.herminen.mapper.JoinMapper;
import com.herminen.outputformat.FilterRecordFormat;
import com.herminen.reducer.FilterReducer;
import com.herminen.reducer.JoinReduce;
import com.herminen.writable.OrderTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created on 2021/3/3.
 *
 * @author ${AUTHOR}
 */
public class JoinTask {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //
        Configuration configuration = new Configuration();
//windows平台也能调度
//        conf.set("mapreduce.app-submission.cross-platform","true");
//        System.setProperty("HADOOP_USER_NAME", "root");
        //调度任务类
        Job job = Job.getInstance(configuration, "joinJob");
        job.setJarByClass(JoinTask.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderTable.class);
        job.setOutputKeyClass(OrderTable.class);
        job.setOutputValueClass(NullWritable.class);
//        FileInputFormat.addInputPath(job, new Path("/herminen/testdata/flowdata/input/"));
//        FileOutputFormat.setOutputPath(job, new Path("/herminen/testdata/flowdata/partition/output/"));
        FileInputFormat.addInputPath(job, new Path("f:/input/join"));
        FileSystem fileSystem = FileSystem.get(configuration);
        Path output = new Path("f:/output/join");
        if(fileSystem.exists(output)){
            fileSystem.delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
