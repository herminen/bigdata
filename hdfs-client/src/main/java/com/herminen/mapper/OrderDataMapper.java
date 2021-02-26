package com.herminen.mapper;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.herminen.writable.OrderBean;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created on 2021/2/26.
 *
 * @author ${AUTHOR}
 */
public class OrderDataMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
    private OrderBean orderBean = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        ArrayList<String> records = Lists.newArrayList(Splitter.on("\t").split(value.toString().trim()));
        if(CollectionUtils.isEmpty(records) || records.size() < 3){
            return;
        }
        orderBean.setOrderId(NumberUtils.toInt(records.get(0)));
        orderBean.setPrice(NumberUtils.toDouble(records.get(2)));
        context.write(orderBean, NullWritable.get());
    }
}
