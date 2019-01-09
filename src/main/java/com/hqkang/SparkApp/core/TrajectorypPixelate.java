package com.hqkang.SparkApp.core;

import java.util.HashMap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;

import com.hqkang.SparkApp.cli.SubmitParser;
import com.hqkang.SparkApp.geom.Point;

public class TrajectorypPixelate {
	public static void main(String[] args) throws Exception {
		SubmitParser parser = new SubmitParser(args);
		parser.parse();
		String filePath = parser.getIPath();
		String outPath = parser.getOPath();

		int k = parser.getSegNum();// grid density, segments number
		
		Builder blder = SparkSession.builder().appName("ImportSeg");

		if (parser.getDebug()) {
			blder.master("local");
		}
		SparkSession spark = blder.getOrCreate();
		spark.conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		spark.conf().set("spark.kryo.registrator", "MyRegistrator");
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		
		PixelizationHelper.Pixelate(sc, new Point(39.7,116.1), new Point(40.1,116.6), filePath, k, 50000, 50000, 60, outPath);
		
		
	}


}
