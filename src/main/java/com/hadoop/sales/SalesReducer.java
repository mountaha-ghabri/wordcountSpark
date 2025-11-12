package com.hadoop.sales;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SalesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    private DoubleWritable totalSales = new DoubleWritable();

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        double sum = 0.0;

        // Sum all price values for the store (key)
        for (DoubleWritable value : values) {
            sum += value.get();
        }

        totalSales.set(sum);
        context.write(key, totalSales);
    }
}