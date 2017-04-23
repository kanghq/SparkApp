package com.hqkang.SparkApp.core;

import java.io.File;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.VoidFunction;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;

import com.vividsolutions.jts.geom.Polygon;

import scala.Tuple2;

public class Import {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// Create a Java Spark Context

		SparkSession spark = SparkSession.builder().appName("wordCount").master("local").getOrCreate();
		spark.conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		spark.conf().set("spark.kryo.registrator", "MyRegistrator");
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		String filePath = "000/Trajectory";
		ResourceBundle rb = ResourceBundle.getBundle("Config");
		int k = 20;
		try {
			filePath = rb.getString("importPath");
			k = Integer.parseInt(rb.getString("k"));

		} catch (MissingResourceException ex) {
		}

		List<File> file = Helper.ReadAllFile(filePath);
		Iterator<File> ite = file.iterator();

		String fileName = ite.next().getPath();
		JavaPairRDD<String, MBRList> mbrRDD = Helper.importFromFile(filePath, sc, k);

		PolygonRDD mypolygonRDD = Helper.transformToPolygonRDD(mbrRDD);

		// databaseRDD.count();
		/*
		 * mypolygonRDD.foreach(new VoidFunction<Polygon>() {
		 * 
		 * @Override public void call(Polygon t) throws Exception { // TODO
		 * Auto-generated method stub System.out.println(t); }
		 * 
		 * });
		 */

		sc.stop();

	}

	private static ArrayList<File> ReadAllFiles() {
		// TODO Auto-generated method stub
		return null;
	}

}