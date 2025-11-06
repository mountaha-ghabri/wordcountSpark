package com.bigdata.wordcountSpark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import java.util.Arrays;

public class WordCountTask {
    // Note: The LOGGER is defined but still unused—it will generate a warning,
    // but we'll leave it in as it was part of the original tutorial structure.
    private static final Logger LOGGER = LoggerFactory.getLogger(WordCountTask.class);

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

        // FIX: Using try-with-resources (Java 7+) ensures sc.stop() is called automatically,
        // removing the warning and the need for the manual finally block.
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {

            JavaRDD<String> textFile = sc.textFile(inputFilePath);

            JavaPairRDD<String, Integer> counts = textFile
                    // 1. Split by whitespace and convert to lowercase
                    .flatMap(s -> Arrays.asList(s.toLowerCase().split("\\s+")).iterator())

                    // 2. CRITICAL FIX: Filter out any 'word' that is a number (price, quantity, date).
                    // This ensures only textual data (like city names) remains for counting.
                    .filter(word -> !word.matches("[0-9.,-]+")) // Added hyphen '-' for dates

                    // 3. Map (word, 1) and Reduce (sum the 1s)
                    .mapToPair(word -> new Tuple2<>(word, 1))
                    .reduceByKey((a, b) -> a + b);

            counts.saveAsTextFile(outputDir);

        }
        // The sc.stop() call is implicitly handled by the try-with-resources block.
    }
}