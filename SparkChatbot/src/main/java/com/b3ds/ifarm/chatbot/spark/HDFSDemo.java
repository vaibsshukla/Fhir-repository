package com.b3ds.ifarm.chatbot.spark;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;


public class HDFSDemo {

	public static void main(String[] args) throws IOException {

		Logger.getLogger("org").setLevel(Level.OFF);
		SparkConf conf = new SparkConf()
                .setAppName("kafka-sandbox")
                .setMaster("local[*]");
		SparkSession session = SparkSession.
				builder().
				config(conf).
				getOrCreate();
		
		FileInputStream fis = new FileInputStream(new File("/home/b3ds/session.json"));
		JavaSparkContext sc = JavaSparkContext.fromSparkContext(session.sparkContext());
		
	}

}
