package org.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class MasterProblem {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("CountMasterOccurrences");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String inputFile = "src/main/resources/master.txt";
        JavaRDD<String> textRDD = sc.textFile(inputFile);

        long masterCount = textRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                .filter(word -> word.toLowerCase().equals("master"))
                .count();

        System.out.println("Count for 'master': " + masterCount);

        sc.stop();
    }
}
