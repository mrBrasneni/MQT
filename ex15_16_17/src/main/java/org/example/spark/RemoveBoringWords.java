package org.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
public class RemoveBoringWords {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RemoveBoringWords");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String file1Path = "src/main/resources/fisier1.txt";
        JavaRDD<String> wordsFile1 = sc.textFile(file1Path).flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());

        String file2Path = "src/main/resources/fisier2.txt";
        JavaRDD<String> boringWords = sc.textFile(file2Path);

        Set<String> boringWordsSet = new HashSet<>(boringWords.collect());

        JavaRDD<String> filteredWords = wordsFile1.filter(word -> !boringWordsSet.contains(word));

        String outputPath = "src/main/resources/rezultat.txt";
        filteredWords.saveAsTextFile(outputPath);

        sc.stop();
    }
}
