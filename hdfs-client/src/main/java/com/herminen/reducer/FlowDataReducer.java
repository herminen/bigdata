package com.herminen.reducer;

import com.herminen.writable.FlowBeanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created on 2021/2/23.
 *
 * @author ${AUTHOR}
 */
public class FlowDataReducer extends Reducer<Text, FlowBeanWritable, Text, FlowBeanWritable> {

    @Override
    protected void reduce(Text key, Iterable<FlowBeanWritable> values, Context context) throws IOException, InterruptedException {
        Long sumUpFlow = 0L, sumDonwFlow  = 0L;
        for (FlowBeanWritable value : values) {
            sumUpFlow += value.getUpFlow();
            sumDonwFlow = value.getDownFlow();
        }
        context.write(key, new FlowBeanWritable(sumUpFlow, sumDonwFlow, (sumUpFlow + sumDonwFlow)));
    }
}
