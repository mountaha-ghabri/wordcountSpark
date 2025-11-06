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
        JavaSparkContext sc = new JavaSparkContext(conf);

        try {
            JavaRDD<String> textFile = sc.textFile(inputFilePath);

            JavaPairRDD<String, Integer> counts = textFile
                    .flatMap(s -> Arrays.asList(s.toLowerCase().split("\\s+")).iterator())

                    // NEW: Filter out any 'word' that is a pure number (or number with a dot/comma)
                    .filter(word -> !word.matches("[0-9.,]+"))

                    .mapToPair(word -> new Tuple2<>(word, 1))
                    .reduceByKey((a, b) -> a + b);

            counts.saveAsTextFile(outputDir);

        } finally {
            sc.stop();
        }
    }
}