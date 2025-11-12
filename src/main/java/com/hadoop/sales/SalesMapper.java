package com.hadoop.sales;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text store = new Text();
    private DoubleWritable price = new DoubleWritable();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        // Split by one or more whitespace characters (\s+) for robustness
        String[] fields = line.split("\\s+");

        // Structure: Date Time Store Product Price Card
        if (fields.length >= 5) { // Check that Store (index 2) and Price (index 4) exist
            try {
                String storeId = fields[2];
                double priceValue = Double.parseDouble(fields[4]);

                store.set(storeId);
                price.set(priceValue);
                context.write(store, price);
            } catch (NumberFormatException e) {
                // Ignore lines with invalid non-numeric price data
            }
        }
    }
}