package com.herminen.reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created on 2021/3/2.
 *
 * @author ${AUTHOR}
 */
public class FilterReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    private Text key  = new Text();
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String line = key.toString();

        line += "\r\n";

        key.set(line);

        context.write(key, NullWritable.get());
    }
}
