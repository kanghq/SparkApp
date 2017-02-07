package com.hqkang.SparkApp.core;


import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import scala.Tuple2;

public class Import {
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// Create a Java Spark Context
		
		SparkSession spark = SparkSession.builder().appName("wordCount").getOrCreate();
		spark.conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		spark.conf().set("spark.kryo.registrator", "MyRegistrator");
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		// Load our input data.
		String filePath  = "000/Trajectory";
		ResourceBundle rb = ResourceBundle.getBundle("Config");
		int k=20;
		try{
			filePath  = rb.getString("importPath");
			k  = Integer.parseInt(rb.getString("k"));

		} 		catch(MissingResourceException ex){}
		


		
		
		List<File> file = 	Helper.ReadAllFile(filePath);
		Iterator<File> ite = file.iterator();
		
		String fileName = ite.next().getPath();
		JavaPairRDD<String, MBRList> mbrRDD =  Helper.importFromFile(fileName, sc, k);
		mbrRDD.count();
		while(ite.hasNext()) {
			
			fileName = ite.next().getPath();
			JavaPairRDD<String, MBRList> newRDD = Helper.importFromFile(fileName, sc, k).cache();
			//newRDD.count();
			mbrRDD =  mbrRDD.union(newRDD).cache();
			//mbrRDD.count();
			
		}
		
		/////////////////////////
	/*	JavaRDD<File> fileRDD = sc.parallelize(file);
		JavaPairRDD<String, MBRList> mbrRDD =  fileRDD.map(new Function<File, JavaPairRDD<String, MBRList>>(){

			@Override
			public JavaPairRDD<String, MBRList> call(File v1) throws Exception {
				// TODO Auto-generated method stub
				JavaPairRDD<String, MBRList> mbrRDD =  Helper.importFromFile(v1.toString());
				return mbrRDD;
				}
			
		}).reduce(new Function2<JavaPairRDD<String,MBRList>,JavaPairRDD<String,MBRList>,JavaPairRDD<String,MBRList>>() {

			@Override
			public JavaPairRDD<String, MBRList> call(JavaPairRDD<String, MBRList> v1, JavaPairRDD<String, MBRList> v2)
					throws Exception {
				// TODO Auto-generated method stub
				return v1.union(v2);
			}});*/
		//////////////////////
		

	
		JavaPairRDD<Tuple2<Integer, String>, MBR> databaseRDD = Helper.store2DB(mbrRDD).cache();

		databaseRDD.foreach(new VoidFunction<Tuple2<Tuple2<Integer,String>,MBR>>() {

			

			@Override
			public void call(Tuple2<Tuple2<Integer, String>, MBR> t) throws Exception {				// TODO Auto-generated method stub
				System.out.println(t._1+ "----" +t._2);
				
			}

		
				
			}); 
		//databaseRDD.count(); 
	
	
		sc.stop();
	
	}

	private static ArrayList<File> ReadAllFiles() {
		// TODO Auto-generated method stub
		return null;
	}
  

}