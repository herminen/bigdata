package com.herminen.reducer;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created on 2021/2/23.
 *
 * @author ${AUTHOR}
 */
public class SequenceFileReducer extends Reducer {
    @Override
    protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
        context.write(key, values.iterator().next());
    }
}
