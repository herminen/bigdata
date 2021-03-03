package com.herminen.mapper;

import com.herminen.writable.OrderTable;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Created on 2021/3/3.
 *
 * @author ${AUTHOR}
 */
public class DistriCacheJoinMapper extends Mapper<LongWritable, Text, OrderTable, NullWritable> {

    Map<String, String> pdMap = new HashMap<String, String>();

    OrderTable orderTable  = new OrderTable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
        String line = "";
        while(StringUtils.isNoneBlank(line = bufferedReader.readLine())){
            String[] splits = line.split("\t");
            //记录缓存
            pdMap.put(splits[0], splits[1]);
        }
        IOUtils.closeStream(bufferedReader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        orderTable.setOrder_id(split[0]);
        orderTable.setP_id(split[1]);
        orderTable.setAmount(NumberUtils.toInt(split[2]));
        orderTable.setPname(pdMap.get(orderTable.getP_id()));

        context.write(orderTable, NullWritable.get());
    }
}
