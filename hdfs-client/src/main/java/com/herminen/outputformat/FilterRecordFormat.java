package com.herminen.outputformat;

import com.herminen.recordwriter.FilterRecordWriter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created on 2021/3/2.
 *
 * @author ${AUTHOR}
 */
public class FilterRecordFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job){
        return new FilterRecordWriter(job);
    }
}
