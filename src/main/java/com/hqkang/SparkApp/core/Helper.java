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
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.RangePartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.neo4j.gis.spatial.EditableLayer;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.graphdb.Transaction;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;

import scala.Tuple2;

public class Helper{
	
	public static ArrayList<File> ReadAllFile(String filePath) {  
        File f = null;  
        f = new File(filePath);  
        File[] files = f.listFiles(); // get all files from f folder
        ArrayList<File> list = new ArrayList<File>();  
        for (File file : files) {  
            if(file.isDirectory()) {  
                //if file is a directory，read its files recursively
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
		//inputData.count();
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
								if(pt.getTime()==null) {
									System.err.println("Error Point"+pt);
								}
								LinkedList<Point> ls = new LinkedList<Point>();
								ls.add(pt);
								return new Tuple2(fileName, ls); 
								
							}
							
						}).cache();
				Partitioner p = new HashPartitioner(5);
				points.partitionBy(p);

				//points.count();
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
				//pointsList.count();
				
				JavaPairRDD<String, MBRList> mbrRDD = pointsList.mapToPair(
						new PairFunction<Tuple2<String, LinkedList<Point>>, String, MBRList>() {

							public Tuple2<String, MBRList> call(Tuple2<String, LinkedList<Point>> t) throws Exception {
								return MBRList.segmentationFromPoints(k, t);
							}
							
						}
						).cache();
				//mbrRDD.count();
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
						        String[] property = {"TraID","Seq", "StartTime", "EndTime"};
						        Object[] propertyField = {ele.getTraID(), ele.getSeq(), ele.getTMin(), ele.getTMax()};
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
	  
	  public static void retrieve(String queryFile, JavaSparkContext sc, int k) {

			
			JavaPairRDD<String, MBRList> queRDD =  Helper.importFromFile(queryFile, sc, k);
			
			JavaPairRDD<Tuple2, MBR> queryRDD = Helper.toTupleKey(queRDD);
			
		
	      
			JavaPairRDD<String, Tuple2> resultRDD = queryRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Tuple2, MBR>, String, Tuple2>() {



				@Override
				public Iterator<Tuple2<String, Tuple2>> call(Tuple2<Tuple2, MBR> t) throws Exception {
					// TODO Auto-generated method stub
					MBR queryMBR = t._2;
					System.out.println("Query ID: "+ t._1._2);
		            ArrayList<Tuple2<String, Tuple2>> list = new ArrayList<Tuple2<String, Tuple2>>();

					try (Transaction tx = new Neo4JCon().getDb().beginTx()) {
			        	List<SpatialDatabaseRecord> results = null;
			        	Envelope env = new Envelope(queryMBR.getXMin(), queryMBR.getXMax(), queryMBR.getYMin(), queryMBR.getYMax());

			        	GeoPipeline pip = GeoPipeline
			                    .startIntersectSearch(new Neo4JCon().getLayer(), new Neo4JCon().getLayer().getGeometryFactory().toGeometry(env))
			                   ;
			        	try{
			        	results = pip
			                    .toSpatialDatabaseRecordList();
			        	} catch(Exception e) {
			        		e.printStackTrace();
			        	}
			            for ( SpatialDatabaseRecord r : results )
				        {
				           // System.out.println( "\t\tGeometry: " + r );
				            Geometry queriedGeo = r.getGeometry();
				            Polygon section = null;
				            Double vol = 0.0;
				            Geometry intersecRes = null;
				            try {
					            Polygon queriedPol =  new Neo4JCon().getLayer().getGeometryFactory().createPolygon(queriedGeo.getCoordinates());
					            String TraID = (String) r.getProperty("TraID");
					            String Seq = (String) r.getProperty("Seq");
					            Double startTime = (Double) r.getProperty("StartTime");
					            Double endTime = (Double) r.getProperty("EndTime");
					            Tuple2 resultMBR = new Tuple2(Seq,TraID);
					            
					           // System.out.println("Inters Obj"+queriedPol);
					            if(startTime< queryMBR.getTMax() && endTime > queryMBR.getTMin()) {
						            intersecRes = queriedPol.intersection(new Neo4JCon().getLayer().getGeometryFactory().createPolygon(queryMBR.shape()));
						            
					            	section = (Polygon) intersecRes;
						         //   System.out.println("Inters NEW obj"+section);
						            vol = section.getArea();
					            } else { vol = 0.0;}
					            
					            list.add(new Tuple2(TraID, new Tuple2(vol, resultMBR)));
				            } catch(ClassCastException | IllegalArgumentException e) {
				            	
				            	System.err.println("queriedGeo"+queriedGeo);
				            	System.err.println("intersecRes:"+intersecRes);
				            	
				            	vol = 0.0;
				            }
				            
				            
				            
				            
				        }

			            tx.success();
			        }
					
					return list.iterator();
				}
				
			});
			

	        JavaPairRDD<String, Double> canRDD = resultRDD.aggregateByKey(new Double(0.0), new Function2<Double, Tuple2, Double>() {

				@Override
				public Double call(Double v1, Tuple2 v2) throws Exception {
					// TODO Auto-generated method stub
					return v1 + (Double)v2._1;
				}}, new Function2<Double, Double, Double>() {

					@Override
					public Double call(Double v1, Double v2) throws Exception {
						// TODO Auto-generated method stub
						return v1+v2;
					}
					
				});
	        canRDD.foreach(new VoidFunction<Tuple2<String, Double>>(){

				

				@Override
				public void call(Tuple2<String, Double> t) throws Exception {
					// TODO Auto-generated method stub
					System.out.println(t._1+ " --"+ t._2);
					
				}});
	  }

}
