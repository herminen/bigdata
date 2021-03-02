package com.herminen.job;


import com.herminen.mapper.FilterMapper;
import com.herminen.mapper.FlowDataMapper;
import com.herminen.outputformat.FilterRecordFormat;
import com.herminen.partition.ProvincePartition;
import com.herminen.reducer.FilterReducer;
import com.herminen.reducer.FlowDataReducer;
import com.herminen.writable.FlowBeanWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created on 2021/3/2.
 *
 * @author ${AUTHOR}
 */
public class FilterTask {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        //windows平台也能调度
//        conf.set("mapreduce.app-submission.cross-platform","true");
//        System.setProperty("HADOOP_USER_NAME", "root");
        //调度任务类
        Job job = Job.getInstance(configuration, "filterJob");
        job.setJarByClass(FilterTask.class);
        job.setMapperClass(FilterMapper.class);
        job.setCombinerClass(FilterReducer.class);
        job.setReducerClass(FilterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
//        FileInputFormat.addInputPath(job, new Path("/herminen/testdata/flowdata/input/"));
//        FileOutputFormat.setOutputPath(job, new Path("/herminen/testdata/flowdata/partition/output/"));
        job.setOutputFormatClass(FilterRecordFormat.class);
        FileInputFormat.addInputPath(job, new Path("f:/input/filter"));
        // 虽然我们自定义了outputformat，但是因为我们的outputformat继承自fileoutputformat
        // 而fileoutputformat要输出一个_SUCCESS文件，所以，在这还得指定一个输出目录
        FileOutputFormat.setOutputPath(job, new Path("f:/output/filter"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
