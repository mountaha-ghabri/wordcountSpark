package com.bigdata.wordcountSpark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;

// Removed unused imports and the unused static final LOGGER field to eliminate warnings.

public class WordCountTask {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Please provide the path of input file and output dir as parameters.");
            System.exit(1);
        }
        new WordCountTask().run(args[0], args[1]);
    }

    public void run(String inputFilePath, String outputDir) {
        SparkConf conf = new SparkConf()
                .setAppName(WordCountTask.class.getName());

        // Use try-with-resources to automatically close the SparkContext.
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            JavaRDD<String> textFile = sc.textFile(inputFilePath);

            JavaPairRDD<String, Integer> counts = textFile
                    // 1. Split by whitespace and convert to lowercase
                    .flatMap(s -> Arrays.asList(s.toLowerCase().split("\\s+")).iterator())

                    // 2. FINAL CRITICAL FIX: Filter to include ONLY letters (a-z).
                    // This is the cleanest way to exclude all numbers, prices, times, and dates.
                    .filter(word -> word.matches("[a-z]+"))

                    // 3. Map (word, 1) and Reduce (sum the 1s)
                    .mapToPair(word -> new Tuple2<>(word, 1))
                    .reduceByKey((a, b) -> a + b);

            counts.saveAsTextFile(outputDir);
        }
    }
}