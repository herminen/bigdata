package com.herminen.reducer;

import com.google.common.collect.Lists;
import com.herminen.writable.OrderTable;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * Created on 2021/3/3.
 *
 * @author ${AUTHOR}
 */
public class JoinReduce extends Reducer<Text, OrderTable, OrderTable, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<OrderTable> values, Context context) throws IOException, InterruptedException {
        List<OrderTable> orderTables = Lists.newArrayList();

        OrderTable pdBean = new OrderTable();

        for (OrderTable orderTable : values) {
            if(orderTable.isOrderRecord()){
                OrderTable orderBean = new OrderTable();
                try {
                    BeanUtils.copyProperties(orderBean, orderTable);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderTables.add(orderBean);
            }else{
                try {
                    BeanUtils.copyProperties(pdBean, orderTable);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        for (OrderTable orderTable : orderTables) {
            orderTable.setPname(pdBean.getPname());
            context.write(orderTable, NullWritable.get());
        }
    }
}
