package com.hqkang.SparkApp.core;



import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;

import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.StatCounter;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.graphdb.Transaction;
import org.neo4j.spark.Neo4JavaSparkContext;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import com.vividsolutions.jts.geom.Polygon;

import scala.Tuple2;

public class Retrieve {
	
	
	public static void main(String[] args) {
		
		// TODO Auto-generated method stub
		// Create a Java Spark Context
		SparkSession spark = SparkSession.builder().appName("wordCount").master("local").config("spark.neo4j.bolt.url", "25519173").getOrCreate();
		//spark.conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		//spark.conf().set("spark.kryo.registrator", "MyRegistrator");
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		Neo4JavaSparkContext csc = Neo4JavaSparkContext.neo4jContext(sc);
		// Load our input data.
		ResourceBundle rb = ResourceBundle.getBundle("Config");
		int k =20;
		String queryFile = "20081024020959.plt";
		String filePath  = "000/Trajectory";
		
		try{
		    queryFile = rb.getString("queryFile");
		    queryFile = rb.getString("importPath");

			k  = Integer.parseInt(rb.getString("k"));

		}
		catch(MissingResourceException ex){}
	
		List<File> file = 	Helper.ReadAllFile(filePath);
		Iterator<File> ite = file.iterator();
		
		String fileName = ite.next().getPath();
		
		
		JavaPairRDD<String, Tuple2<Double,Boolean>> retRDD = Helper.retrieve(filePath, sc, k);
		
		RowMatrix res = Helper.PCA(retRDD,file.size(), sc, file);
		
		List<Vector> vs = res.rows().toJavaRDD().collect();
		for(Vector v: vs) {
		    System.out.println(v);
		}
		
		
		
        
		
		sc.stop();
	
	}

  

}