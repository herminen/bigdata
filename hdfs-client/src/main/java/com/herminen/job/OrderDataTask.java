package com.herminen.job;

import com.herminen.groupcomparator.OrderSortGroupComparator;
import com.herminen.mapper.FlowDataMapper;
import com.herminen.mapper.OrderDataMapper;
import com.herminen.partition.ProvincePartition;
import com.herminen.reducer.FlowDataReducer;
import com.herminen.reducer.OrderDataReducer;
import com.herminen.writable.FlowBeanWritable;
import com.herminen.writable.OrderBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created on 2021/2/26.
 *
 * @author ${AUTHOR}
 */
public class OrderDataTask {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        //windows平台也能调度
//        conf.set("mapreduce.app-submission.cross-platform","true");
//        System.setProperty("HADOOP_USER_NAME", "root");
//        conf.set("dfs.permissions", "false");
        //调度任务类
        Job job = Job.getInstance(conf, "orderDataJob");
        job.setJarByClass(OrderDataTask.class);
        job.setMapperClass(OrderDataMapper.class);
        job.setReducerClass(OrderDataReducer.class);
        // 4 设置map输出数据key和value类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 5 设置最终输出数据的key和value类型
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        // 8 设置reduce端的分组
        job.setGroupingComparatorClass(OrderSortGroupComparator.class);

        FileInputFormat.addInputPath(job, new Path("f:/input/order/"));
        FileOutputFormat.setOutputPath(job, new Path("f:/output/order/"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
