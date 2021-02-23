package com.herminen.recordreader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created on 2021/2/23.
 *
 * @author ${AUTHOR}
 */
public class WholeRecordReader extends RecordReader<Text, BytesWritable> {

    private Configuration configuration;
    private FileSplit split;

    private boolean isProgress = true;
    private BytesWritable value = new BytesWritable();
    private Text key = new Text();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        this.split = (FileSplit) split;
        this.configuration = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(isProgress) {
            byte[] content  = new byte[(int) split.getLength()];

            FileSystem fs = null;
            FSDataInputStream fis = null;
            try {
                fs = FileSystem.get(configuration);
                Path path = split.getPath();
                fis = fs.open(path);

                //拷贝数据
                IOUtils.readFully(fis, content, 0, (int) split.getLength());

                value.set(content, 0, content.length);
                //获取 文件 路劲及名称
                String name = split.getPath().getName();

                key.set(name);

            } catch (Exception e) {

            }finally {
                IOUtils.closeStream(fis);
            }
            isProgress = false;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return this.value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
