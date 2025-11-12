package com.hadoop.charcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CharCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text character = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Convert line to string and lowercase it for case-insensitive counting
        String line = value.toString().toLowerCase();

        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);

            // Only process alphabetic characters, ignoring spaces, numbers, etc.
            if (Character.isLetter(ch)) {
                character.set(String.valueOf(ch));
                context.write(character, one); // Emit (letter, 1)
            }
        }
    }
}