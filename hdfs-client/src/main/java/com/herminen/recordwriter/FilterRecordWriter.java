package com.herminen.recordwriter;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Created on 2021/3/2.
 *
 * @author ${AUTHOR}
 */
public class FilterRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream atguiguOutputStream, otherOutputStream;

    public FilterRecordWriter(TaskAttemptContext context) {
        try {
            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            atguiguOutputStream = fileSystem.create(new Path("f:/output/filter/atguigu"));
            otherOutputStream = fileSystem.create(new Path("f:/output/filter/other"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void write(Text key, NullWritable value) throws IOException {
        if(key.toString().contains("atguigu")){
            atguiguOutputStream.write(key.toString().getBytes(Charset.forName("UTF-8")));
        }else {
            otherOutputStream.write(key.toString().getBytes(Charset.forName("UTF-8")));
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(atguiguOutputStream);
        IOUtils.closeStream(otherOutputStream);
    }
}
