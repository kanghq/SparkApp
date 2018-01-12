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
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.babylon.core.ImageGenerator;
import org.datasyslab.babylon.extension.imageGenerator.BabylonImageGenerator;
import org.datasyslab.babylon.extension.visualizationEffect.HeatMap;
import org.datasyslab.babylon.utils.ImageType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialRDD.LineStringRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;

import com.hqkang.SparkApp.cli.GeoSparkParser;
import com.hqkang.SparkApp.cli.SubmitParser;
import com.hqkang.SparkApp.geom.MBR;
import com.hqkang.SparkApp.geom.MBRList;
import com.hqkang.SparkApp.geom.MBRRDDKey;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;

import scala.Tuple2;

public class Import {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		// Create a Java Spark Context

		GeoSparkParser parser = new GeoSparkParser(args);
		parser.parse();
		// String filePath = "000/Trajectory";
		// ResourceBundle rb = ResourceBundle.getBundle("Config");
		String filePath = parser.getIPath();
		int k = parser.getSegNum();
		int margin = parser.getMargin();
		boolean SaveAll = parser.getSaveAll();
		boolean stat = parser.getStat();

		String outputPath = parser.getOPath();
		int part = parser.getPart();
		Builder blder = SparkSession.builder().appName("ImportSeg");

		if (parser.getDebug()) {
			blder.master("local");
		}
		SparkSession spark = blder.getOrCreate();
		spark.conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		spark.conf().set("spark.kryo.registrator", "MyRegistrator");
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		// sc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId",
		// parser.getAccessID());
		// sc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey",
		// parser.getSecretKey()); // can contain "/"

		// List<File> file = Helper.ReadAllFile(filePath);
		// Iterator<File> ite = file.iterator();

		// String fileName = ite.next().getPath();
		JavaPairRDD<String, MBRList> mbrRDD = CommonHelper.importFromFile(filePath, sc, k, part);
		//LineStringRDD myLSRDD = CommonHelper.importLSTra(filePath, sc, k, part);
		//System.out.println(myLSRDD.countWithoutDuplicates());
		
		JavaPairRDD<MBRRDDKey, MBR> dbrdd = GeoSparkHelper.toDBRDD(mbrRDD, margin);
		dbrdd.persist(StorageLevel.MEMORY_ONLY_SER());
		String currentPath = outputPath+System.currentTimeMillis()+"/";
		
		PolygonRDD mypolygonRDD = GeoSparkHelper.transformToPolygonRDD(dbrdd, margin);
		//PolygonRDD mypolygonRDDWOP = GeoSparkHelper.transformToPolygonRDDWOPartition(dbrdd, margin);
		
		
		
		//Envelope BJBoundary = new Envelope(116.11,116.58,39.77,40.02);
		//HeatMap visualizationOperator = new HeatMap(1000,800,BJBoundary,true,20);
		//visualizationOperator.Visualize(sc, mypolygonRDDWOP);
		//visualizationOperator.Visualize(sc, myLSRDD);
		
		//BabylonImageGenerator imageGenerator = new  BabylonImageGenerator();
		//imageGenerator.SaveRasterImageAsHadoopFile(visualizationOperator.rasterImage, currentPath,ImageType.GIF);
		
		/*

		if(stat) {
		List<Long> statList = new ArrayList<Long>();
		Long num = mypolygonRDD.spatialPartitionedRDD.count();
		statList.add(num);
		JavaRDD<Long> statRDD =  sc.parallelize(statList);
		
		statRDD.saveAsTextFile(currentPath+"/stat/");
		} else {

		if(parser.getDebug()) { 
			System.out.println(dbrdd.count());
		}
		*/
		
		/*
		 * mypolygonRDD.foreach(new VoidFunction<Polygon>() {
		 * 
		 * @Override public void call(Polygon t) throws Exception { // TODO
		 * Auto-generated method stub System.out.println(t); }
		 * 
		 * });
		 */
		JavaPairRDD<String, Tuple2<Double, Boolean>> resultRDD =GeoSparkHelper.retrieve(mypolygonRDD, SaveAll, margin,dbrdd);
		//System.out.println(resultRDD.count());
		resultRDD.saveAsTextFile(currentPath);
		
		
		sc.stop();
	}

	}



