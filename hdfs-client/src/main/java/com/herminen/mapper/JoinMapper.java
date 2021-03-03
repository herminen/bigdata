package com.herminen.mapper;

import com.herminen.writable.OrderBean;
import com.herminen.writable.OrderTable;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created on 2021/3/3.
 *
 * @author ${AUTHOR}
 */
public class JoinMapper extends Mapper<LongWritable, Text, Text, OrderTable> {

    String name;
    OrderTable bean = new OrderTable();
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        name = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //FileInputFormat读取文件中的一行
        String line = value.toString();

        //判断来自哪个文件
        String[] split = line.split("\t");
        if(name.startsWith("order")){
            bean.setOrder_id(split[0]);
            bean.setP_id(split[1]);
            bean.setAmount(NumberUtils.toInt(split[2]));
            bean.setPname("");
            bean.setFlag("order");
        }else{
            bean.setP_id(split[0]);
            bean.setPname(split[1]);
            bean.setOrder_id("");
            bean.setAmount(0);
            bean.setFlag("pd");
        }
        k.set(bean.getP_id());
        context.write(k, bean);
    }
}
