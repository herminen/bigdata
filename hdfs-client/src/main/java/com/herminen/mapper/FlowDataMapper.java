package com.herminen.mapper;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.herminen.writable.FlowBeanWritable;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created on 2021/2/23.
 *
 * @author ${AUTHOR}
 */
public class FlowDataMapper extends Mapper<LongWritable, Text, Text, FlowBeanWritable> {

    private Text phone = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String content = value.toString();
        Iterable<String> split = Splitter.on("\t").split(content);
        ArrayList<String> record = Lists.newArrayList(split);
        if(record.size()  < 6){
            return;
        }
        phone.set(record.get(1));
        Long upFlow = NumberUtils.toLong(record.get(4));
        Long downFlow = NumberUtils.toLong(record.get(5));
        context.write(phone, new FlowBeanWritable(upFlow, downFlow));
    }
}
