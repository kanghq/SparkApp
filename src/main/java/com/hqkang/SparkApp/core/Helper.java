package com.hqkang.SparkApp.core;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.neo4j.gis.spatial.EditableLayer;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.graphdb.Transaction;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;

import scala.Tuple2;

public class Helper{
	
	public static ArrayList<File> ReadAllFile(String filePath) {  
        File f = null;  
        f = new File(filePath);  
        File[] files = f.listFiles(); // 得到f文件夹下面的所有文件。  
        ArrayList<File> list = new ArrayList<File>();  
        for (File file : files) {  
            if(file.isDirectory()) {  
                //如何当前路劲是文件夹，则循环读取这个文件夹下的所有文件  
                ReadAllFile(file.getAbsolutePath());
            } else {
            	if(file.getName().endsWith("plt")) {
            		list.add(file);  
            	}
            }  
        } 
        
        return list;
        
       
    } 
	
	/*
	public static JavaPairRDD<String, MBRList> importFromFile(Path fileName) {
		
		FSDataInputStream input = FileSystem.open(fileName);
		input.
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
				JavaPairRDD<String,LinkedList<Point>> points = inputData.mapToPair(
						new PairFunction<String, String, LinkedList<Point>>() {

							public Tuple2<String, LinkedList<Point>> call(String line) throws Exception {
								String[] parts = line.split(",");
								String lat = parts[0];
								String lon = parts[1];
								String sDate = parts[5];
								Date   date;
								String sTime = parts[6];
								Date time;
								Point pt = new Point(sDate, sTime, lat, lon);
								LinkedList<Point> ls = new LinkedList<Point>();
								ls.add(pt);
								return new Tuple2(fileName, ls); 
								
							}
							
						});
				// Transform into pairs and count.
				
				JavaPairRDD<String, LinkedList<Point>> pointsList = points.reduceByKey(new Function2<LinkedList<Point>, LinkedList<Point>, LinkedList<Point>>() {

					@Override
					public LinkedList<Point> call(LinkedList<Point> v1, LinkedList<Point> v2) throws Exception {
						// TODO Auto-generated method stub
						v1.add(v2.pop());
						Collections.sort(v1);
						return v1;
					}
					
				}).cache();
				
				JavaPairRDD<String, MBRList> mbrRDD = pointsList.mapToPair(
						new PairFunction<Tuple2<String, LinkedList<Point>>, String, MBRList>() {

							public Tuple2<String, MBRList> call(Tuple2<String, LinkedList<Point>> t) throws Exception {
								return MBRList.segmentationFromPoints(20, t);
							}
							
						}
						).cache();
				return mbrRDD;
		
	}
	*/
public static JavaPairRDD<String, MBRList> importFromFile(String fileName, JavaSparkContext sc, int k) {

		JavaRDD<String> input = sc.textFile(fileName);
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
				}, false).cache();
		inputData.count();
				JavaPairRDD<String,LinkedList<Point>> points = inputData.mapToPair(
						new PairFunction<String, String, LinkedList<Point>>() {

							public Tuple2<String, LinkedList<Point>> call(String line) throws Exception {
								String[] parts = line.split(",");
								String lat = parts[0];
								String lon = parts[1];
								String sDate = parts[5];
								Date   date;
								String sTime = parts[6];
								Date time;
								Point pt = new Point(sDate, sTime, lat, lon);
								LinkedList<Point> ls = new LinkedList<Point>();
								ls.add(pt);
								return new Tuple2(fileName, ls); 
								
							}
							
						}).cache();
				points.count();
				// Transform into pairs and count.
				
				JavaPairRDD<String, LinkedList<Point>> pointsList = points.reduceByKey(new Function2<LinkedList<Point>, LinkedList<Point>, LinkedList<Point>>() {

					@Override
					public LinkedList<Point> call(LinkedList<Point> v1, LinkedList<Point> v2) throws Exception {
						// TODO Auto-generated method stub
						v1.add(v2.pop());
						Collections.sort(v1);
						return v1;
					}
					
				}).cache();
				pointsList.count();
				
				JavaPairRDD<String, MBRList> mbrRDD = pointsList.mapToPair(
						new PairFunction<Tuple2<String, LinkedList<Point>>, String, MBRList>() {

							public Tuple2<String, MBRList> call(Tuple2<String, LinkedList<Point>> t) throws Exception {
								return MBRList.segmentationFromPoints(k, t);
							}
							
						}
						).cache();
				mbrRDD.count();
				return mbrRDD;
		
	}

	public static JavaPairRDD<Tuple2<Integer, String>, MBR> store2DB(JavaPairRDD<String, MBRList> mbrRDD) {
		JavaPairRDD<Tuple2<Integer, String>, MBR> databaseRDD = mbrRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String, MBRList>, Tuple2<Integer, String>, MBR>() {
					public Iterator<Tuple2<Tuple2<Integer, String>, MBR>> call(Tuple2<String, MBRList> t) {
						Iterator<MBR> ite = t._2.iterator();
						int i = 0;
						List<Tuple2<Tuple2<Integer, String>, MBR>> list = new ArrayList<Tuple2<Tuple2<Integer, String>, MBR>>();
						try (Transaction tx = new Neo4JCon().getDb().beginTx()) {
							while(ite.hasNext()) {
								
								 MBR ele = ite.next();
								 ele.setSeq(Integer.toString(i));
								 ele.setTraID(t._1);
								
						        SerializedEL traLayer = new Neo4JCon().getLayer();
						        LinearRing shell = ele.shape();
						        Polygon pol = traLayer.getGeometryFactory().createPolygon(shell);
						        String[] property = {"TraID","Seq"};
						        Object[] propertyField = {ele.getTraID(),ele.getSeq()};
						        traLayer.add(pol, property, propertyField);
						        Tuple2 idx = new Tuple2<Integer, String>(i, t._1);
						        
								list.add(new Tuple2<Tuple2<Integer, String>, MBR>(idx, ele));
								i++;
							}
							 tx.success();
						}
						return list.iterator();
												
					}
				}
		).cache();
		return databaseRDD;
	}
	
	public static JavaPairRDD<Tuple2, MBR> toTupleKey(JavaPairRDD<String, MBRList> mbrRDD) {
		JavaPairRDD<Tuple2, MBR> databaseRDD = mbrRDD.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String, MBRList>, Tuple2, MBR>() {
					public Iterator<Tuple2<Tuple2, MBR>> call(Tuple2<String, MBRList> t) {
						Iterator<MBR> ite = t._2.iterator();
						int i = 0;
						List<Tuple2<Tuple2, MBR>> list = new ArrayList<Tuple2<Tuple2, MBR>>();
						while(ite.hasNext()) {
							
							MBR ele = ite.next();
							ele.setSeq(Integer.toString(i));
							ele.setTraID(t._1);
							Tuple2 idx = new Tuple2(i, t._1);
							list.add(new Tuple2(idx, ele));
							i++;
						}
						return list.iterator();
												
					}
				}
		).cache();
		return databaseRDD;
	}
	
	
	  public static void printResults(Layer layer, List<SpatialDatabaseRecord> results)
	    {
	        System.out.println( "\tTesting layer '" + layer.getName() + "' (class "
	                            + layer.getClass() + "), found results: "
	                            + results.size() );
	        for ( SpatialDatabaseRecord r : results )
	        {
	            System.out.println( "\t\tGeometry: " + r );
	            Geometry geo = r.getGeometry();
	            System.out.println(geo);
	        }
	    }

}
