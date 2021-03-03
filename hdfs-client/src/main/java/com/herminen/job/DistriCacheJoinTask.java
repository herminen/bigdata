package com.herminen.job;

import com.herminen.mapper.DistriCacheJoinMapper;
import com.herminen.mapper.JoinMapper;
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
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created on 2021/3/3.
 *
 * @author ${AUTHOR}
 */
public class DistriCacheJoinTask {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration configuration = new Configuration();
//windows平台也能调度
//        conf.set("mapreduce.app-submission.cross-platform","true");
//        System.setProperty("HADOOP_USER_NAME", "root");
        //调度任务类
        Job job = Job.getInstance(configuration, "distriCacheJoinJob");
        job.setJarByClass(DistriCacheJoinTask.class);
        job.setMapperClass(DistriCacheJoinMapper.class);
        job.setOutputKeyClass(OrderTable.class);
        job.setOutputValueClass(NullWritable.class);

        //加载缓存数据
        job.addCacheFile(new URI("file:///f:/input/joincache/inputcache/pd.txt"));
        //Map端Join的逻辑不需要Reduce阶段，设置reduceTask数量为0
        job.setNumReduceTasks(0);
//        FileInputFormat.addInputPath(job, new Path("/herminen/testdata/flowdata/input/"));
//        FileOutputFormat.setOutputPath(job, new Path("/herminen/testdata/flowdata/partition/output/"));
        FileInputFormat.addInputPath(job, new Path("f:/input/joincache/order.txt"));
        FileSystem fileSystem = FileSystem.get(configuration);
        Path output = new Path("f:/output/joincache");
        if(fileSystem.exists(output)){
            fileSystem.delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
