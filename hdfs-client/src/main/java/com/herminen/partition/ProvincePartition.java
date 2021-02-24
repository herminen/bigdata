package com.herminen.partition;

import com.herminen.writable.FlowBeanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created on 2021/2/23.
 *
 * @author ${AUTHOR}
 */
public class ProvincePartition extends Partitioner<Text, FlowBeanWritable> {
    @Override
    public int getPartition(Text text, FlowBeanWritable flowBeanWritable, int numPartitions) {
        int partition = 4;
        String preNum = text.toString().substring(0, 3);
        // 2 判断是哪个省
        if ("136".equals(preNum)) {
            partition = 0;
        }else if ("137".equals(preNum)) {
            partition = 1;
        }else if ("138".equals(preNum)) {
            partition = 2;
        }else if ("139".equals(preNum)) {
            partition = 3;
        }
        return partition;
    }
}
