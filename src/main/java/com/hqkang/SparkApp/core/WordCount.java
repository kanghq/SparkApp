package com.hqkang.SparkApp.core;


import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.TreeMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.datasyslab.geospark.spatialRDD.PointRDD;

import scala.Tuple2;


public class WordCount {
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// Create a Java Spark Context
		SparkConf conf = new SparkConf().setMaster("local").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);
		// Load our input data.
		String fileName  = "20081023025304.plt";
		JavaRDD<String> input = sc.textFile(fileName);
		// Split up into words.

		
		JavaRDD<String> inputData = input.mapPartitionsWithIndex(	//remove header
		new Function2<Integer, Iterator<String>, Iterator<String>>() {

			public Iterator<String> call(Integer ind, Iterator<String> iterator) throws Exception {
				// 
				
				while(ind<=5 && iterator.hasNext()) {
					iterator.next();
					ind++;
					
				} 
				return iterator;
					
			}
		}, false);
		JavaPairRDD<String,Point> points = inputData.mapToPair(
				new PairFunction<String, String, Point>() {

					public Tuple2<String, Point> call(String line) throws Exception {
						String[] parts = line.split(",");
						String lat = parts[0];
						String lon = parts[1];
						String sDate = parts[5];
						Date   date;
						String sTime = parts[6];
						Date time;
						Point pt = new Point(sDate, sTime, lat, lon);
						return new Tuple2(fileName, pt); 
						
					}
					
				});
		// Transform into pairs and count.
		
		JavaPairRDD<String, LinkedList<Point>> pointsList = null;
		JavaPairRDD<String, MBRList> mbrRDD = pointsList.mapToPair(
				new PairFunction<Tuple2<String, LinkedList<Point>>, String, MBRList>() {

					public Tuple2<String, MBRList> call(Tuple2<String, LinkedList<Point>> t) throws Exception {
						int k = 20;
						MBRList mbrPriList = new MBRList();
					
						String fileNmae = t._1();
					
						Iterator ite = t._2().iterator();
						
						while(ite.hasNext()) {
							Point point = (Point)ite.next();
							mbrPriList.add(new MBR(point, point));
						}
						
						while(mbrPriList.size() > k) {
						TreeMap<Integer, Double> priQueue = mbrPriList.calculatePotentialMergeCost();
						mbrPriList.mergeNextOne(priQueue.firstKey().intValue());
						}
						
						return new Tuple2(fileNmae,mbrPriList);
					}
					
				}
				);
		
		
		
		
		
		sc.stop();
	
	}
  

}