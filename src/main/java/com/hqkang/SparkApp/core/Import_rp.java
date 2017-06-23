package com.hqkang.SparkApp.core;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialRDD.DBPolygonRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;

import com.hqkang.SparkApp.cli.GeoSparkParser;
import com.hqkang.SparkApp.cli.SubmitParser;
import com.hqkang.SparkApp.geom.MBR;
import com.hqkang.SparkApp.geom.MBRList;
import com.hqkang.SparkApp.geom.MBRRDDKey;
import com.vividsolutions.jts.geom.Polygon;

import scala.Tuple2;

public class Import_rp {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		// Create a Java Spark Context

		GeoSparkParser parser = new GeoSparkParser(args);
		parser.parse();
		// String filePath = "000/Trajectory";
		// ResourceBundle rb = ResourceBundle.getBundle("Config");
		String filePath = parser.getIPath();
		int k = parser.getSegNum();
		int neoSrv = parser.getNeo4jSrv();
		boolean SaveAll = parser.getSaveAll();
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
		for(int i=0;i<neoSrv;i++) {
		try(Connection con = DriverManager.getConnection("jdbc:neo4j:bolt://DBSRV"+i, "neo4j", "25519173")) {
			String query = "call spatial.addWKTLayer('geom','wkt')";
			con.setAutoCommit(false);
			 try (PreparedStatement stmt = con.prepareStatement(query)) {
				 
			        try (ResultSet rs = stmt.executeQuery()) {
			            while (rs.next()) {
			            	
			                System.out.println(rs.getString(1));
			            }
			        }
			    } catch(Exception e) {
			    	e.printStackTrace();
			    } finally {
			    	con.commit();
			    	con.close();
			    }
		} catch(Exception e) {
			e.printStackTrace();
		} 
		}
		JavaPairRDD<String, MBRList> mbrRDD = CommonHelper.importFromFile(filePath, sc, k, part);
		JavaPairRDD<MBRRDDKey, MBR> dbrdd = GeoSparkHelper.toDBRDD(mbrRDD, neoSrv);
		dbrdd.persist(StorageLevel.MEMORY_ONLY());

		DBPolygonRDD mypolygonRDD = GeoSparkHelper.transformToPolygonRDD(dbrdd, neoSrv);

		if(parser.getDebug()) { 
			System.out.println(dbrdd.count());
		}
		
		/*
		 * mypolygonRDD.foreach(new VoidFunction<Polygon>() {
		 * 
		 * @Override public void call(Polygon t) throws Exception { // TODO
		 * Auto-generated method stub System.out.println(t); }
		 * 
		 * });
		 */
		mypolygonRDD.countWithoutDuplicates();
		JavaPairRDD<String, Tuple2<Double, Boolean>> resultRDD =GeoSparkHelper.retrieve(mypolygonRDD, SaveAll,dbrdd, neoSrv);
		resultRDD.saveAsTextFile(outputPath+System.currentTimeMillis());
		sc.stop();

	}


}
