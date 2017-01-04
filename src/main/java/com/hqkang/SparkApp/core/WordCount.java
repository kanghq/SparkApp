package com.hqkang.SparkApp.core;


import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Function1;
import scala.Tuple2;

public class WordCount {
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// Create a Java Spark Context
		
		SparkSession spark = SparkSession.builder().master("local").appName("wordCount").getOrCreate();
		//spark.conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		//spark.conf().set("spark.kryo.registrator", "MyRegistrator");
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		// Load our input data.
		String fileName  = "20081023025304.plt";
		String queryFile = "20081023025304.plt";
		JavaPairRDD<String, MBRList> mbrRDD =  Helper.importFromFile(fileName, sc);
		

	
		JavaPairRDD<Tuple2<Integer, String>, MBR> databaseRDD = Helper.store2DB(mbrRDD);

		databaseRDD.foreach(new VoidFunction<Tuple2<Tuple2<Integer,String>,MBR>>() {

			

			@Override
			public void call(Tuple2<Tuple2<Integer, String>, MBR> t) throws Exception {				// TODO Auto-generated method stub
				System.out.println(t._1+ "----" +t._2);
				
			}

		
				
			});
		databaseRDD.count(); 

		JavaPairRDD<String, MBRList> queRDD =  Helper.importFromFile(queryFile, sc);
		
		JavaPairRDD<Tuple2, MBR> queryRDD = Helper.toTupleKey(queRDD);
		
		Dataset<Row> databaseDF = spark.createDataFrame(databaseRDD.values(),MBR.class);
		Dataset<Row> queryDF = spark.createDataFrame(queryRDD.values(), MBR.class);
		
		Dataset<Row> joinDF = databaseDF.join(queryDF, "seq");
				/*.filter(new FilterFunction<Row>() {

			@Override
			public boolean call(Row r) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(r.schema());
				System.out.println(r);

				return false;
			}
			
		}).count();
		*/
		
		joinDF.foreach(new ForeachFunction<Row>() {

			

		
			@Override
			public void call(Row t) throws Exception {
				// TODO Auto-generated method stub
				System.out.println(t.schema());
				System.out.println(t);
			}

		
				
			});
		
		
		
		sc.stop();
	
	}
  

}