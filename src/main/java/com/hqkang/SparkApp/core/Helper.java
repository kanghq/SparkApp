package com.hqkang.SparkApp.core;

import java.io.File;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.neo4j.gis.spatial.EditableLayer;
import org.neo4j.gis.spatial.Layer;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.spark.Neo4JavaSparkContext;

import com.alibaba.fastjson.JSON;
import com.google.gson.Gson;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import org.joda.time.*;
import flexjson.JSONDeserializer;
import flexjson.JSONSerializer;
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
	
	public static String conString(String[] list) {
		String str = "[";
		for(int i = 0; i< list.length; i++) {
		
			str += "'";
			str += list[i].toString();
			str += "',";
		
		
		}
		if (str.endsWith(",")) {
		    str = str.substring(0, str.length() - 1) + "]";
		}
		return str;

	}
	
	
public static JavaPairRDD<String, MBRList> importFromFile(String fileName, JavaSparkContext sc, int k) {
		
		JavaPairRDD<String, String> input = sc.wholeTextFiles(fileName);
		JavaPairRDD<String, LinkedList<Point>> points = input.flatMapToPair(
				new PairFlatMapFunction<Tuple2<String, String>, String, LinkedList<Point>>() {

					@Override
					public Iterator<Tuple2<String, LinkedList<Point>>> call(Tuple2<String, String> t) throws Exception {
						// TODO Auto-generated method stub
						ArrayList res = new ArrayList();
						String text = t._2;
						LinkedList<Point> ls = new LinkedList<Point>();
						String line[] = text.split("\n");
						for(int i = 6; i< line.length; i++) {
						
							try{
								String[] parts = line[i].split(",");
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
								ls.add(pt);
								
							} catch(Exception e) {
								
							} 
						}
						res.add(new Tuple2(t._1, ls)); 
						return res.iterator();
						
					}

					
					
				});
		
		
				Partitioner p = new HashPartitioner(5);
				points.partitionBy(p);

				//points.count();

				
				JavaPairRDD<String, MBRList> mbrRDD = points.mapToPair(
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
						try {
							while(ite.hasNext()) {
								
								 MBR ele = ite.next();
								 ele.setSeq(Integer.toString(i));
								 ele.setTraID(t._1);
							     Tuple2 idx = new Tuple2<Integer, String>(i, t._1);

								list.add(new Tuple2<Tuple2<Integer, String>, MBR>(idx, ele));
								i++;
							}
							 
						} catch(Exception e) {}
						return list.iterator();
												
					}
				}
		).cache();
		
		databaseRDD.foreachPartition(
				new VoidFunction<Iterator<Tuple2<Tuple2<Integer,String>,MBR>>>() {

					@Override
					public void call(Iterator<Tuple2<Tuple2<Integer, String>, MBR>> t) throws Exception {
						// TODO Auto-generated method stub
						while(t.hasNext()) {
							Tuple2<Tuple2<Integer, String>, MBR>  tu = t.next();
							MBR ele = tu._2;
							
							
							
					        Polygon pol = ele.shape();
					        String[] property = {"TraID","Seq", "StartTime", "EndTime", "MBRJSON"};
							DecimalFormat df = new DecimalFormat("#");
							
							
							 String json = Gson.class.newInstance().toJson(ele);

					        String[] propertyField = {ele.getTraID(), ele.getSeq(), df.format(ele.getTMin()), df.format(ele.getTMax()), json};
							try(Connection con = DriverManager.getConnection("jdbc:neo4j:bolt://localhost", "neo4j", "25519173")) {
								String query = "CALL spatial.addWKTWithProperties('geom','"+ pol.toText() +"',"+ conString(property)+","+conString(propertyField)+")";
								 try (PreparedStatement stmt = con.prepareStatement(query)) {

								        try (ResultSet rs = stmt.executeQuery()) {
								            while (rs.next()) {
								                System.out.println(rs.getString(1));
								            }
								        }
								    }
							}
						}
						
					}
					
				}
				);
		
		
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

			
			
		
	      
			JavaPairRDD<String, Tuple2> resultRDD = queryRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Tuple2, MBR>>, String, Tuple2>() {



				@Override
				public Iterator<Tuple2<String, Tuple2>> call(Iterator<Tuple2<Tuple2, MBR>> t) throws Exception {
					// TODO Auto-generated method stub
		            ArrayList<Tuple2<String, Tuple2>> list = new ArrayList<Tuple2<String, Tuple2>>();

					while(t.hasNext()) {
						Tuple2<Tuple2, MBR> t1 = t.next();
						MBR queryMBR = t1._2;
	
			            
			            try(Connection con = DriverManager.getConnection("jdbc:neo4j:bolt://localhost", "neo4j", "25519173")) {
				        	Envelope env = new Envelope(queryMBR.getXMin(), queryMBR.getXMax(), queryMBR.getYMin(), queryMBR.getYMax());
				 
				        	String queryStr = "POLYGON((" +queryMBR.getXMin()+" "+queryMBR.getYMin()+", "+
				        							   	   queryMBR.getXMin()+" "+queryMBR.getYMax()+", "+
				        								   queryMBR.getXMax()+" "+queryMBR.getYMax()+", "+
				        								   queryMBR.getXMin()+" "+queryMBR.getYMax()+", "+
				        								   queryMBR.getXMin()+" "+queryMBR.getYMin()+"))";
			            	
			            	String query = "CALL spatial.intersects('geom','"+queryStr+"') YIELD node RETURN node";
							 try (PreparedStatement stmt = con.prepareStatement(query)) {
	
							        try (ResultSet rs = stmt.executeQuery()) {
							        	JSONParser parser = new JSONParser();
							            while (rs.next()) {
							            	String jsonStr = rs.getString(1);
							            	jsonStr = jsonStr.replaceAll("\"\\{\"", "\\{\"");
							            	jsonStr = jsonStr.replaceAll("\"\\}\"", "\"\\}");
							            	Map<String,Object> node=(Map)JSON.parse(jsonStr);  
							            	Map<String, Object> mbrMap = (Map<String, Object>) node.get("MBRJSON");
							            	String mbrjson = JSON.toJSONString(mbrMap);
							            	
							            	MBR iMBR = Gson.class.newInstance().fromJson(mbrjson,MBR.class);
								            Polygon section = null;
								            Double vol = 0.0;
								            Geometry intersecRes = null;
								            try {
									            Polygon queriedPol =  iMBR.shape();
									            String TraID = iMBR.getTraID();
									            String Seq = iMBR.getSeq();
									            Double startTime = iMBR.getTMin();
									            Double endTime = iMBR.getTMax();
									            Tuple2 resultMBR = new Tuple2(Seq,TraID);
									            
									           // System.out.println("Inters Obj"+queriedPol);
									            if(startTime< queryMBR.getTMax() && endTime > queryMBR.getTMin()) {
										            intersecRes = queriedPol.intersection(queryMBR.shape());
										            
									            	section = (Polygon) intersecRes;
										         //   System.out.println("Inters NEW obj"+section);
										            vol = section.getArea();
									            } else { vol = 0.0;}
									            if(vol > 0.0) {
									            	Interval qInterval = new Interval(queryMBR.getTMin().longValue(), queryMBR.getTMax().longValue());
									            	Interval iInterval = new Interval(iMBR.getTMin().longValue(), iMBR.getTMax().longValue());
									            	Interval inters = qInterval.overlap(iInterval);
									            	Long intStart = inters.getStartMillis();
									            	Long intEnd = inters.getEndMillis();
									            	Long intMid = (intStart+intEnd)/2;
									            	Point iStart = iMBR.getInsidePoints().getPtSnp(intStart);
									            	Point qStart = queryMBR.getInsidePoints().getPtSnp(intStart);
									            	Point iMid = iMBR.getInsidePoints().getPtSnp(intMid);
									            	Point qMid = queryMBR.getInsidePoints().getPtSnp(intMid);
									            	Point iEnd = iMBR.getInsidePoints().getPtSnp(intEnd);
									            	Point qEnd = queryMBR.getInsidePoints().getPtSnp(intEnd);

									            }
									            
									            list.add(new Tuple2("QT:"+queryMBR.getTraID()+"__"+TraID, new Tuple2(vol, resultMBR)));
								            } catch(ClassCastException | IllegalArgumentException e) {
								            	
								            	System.err.println("queriedMBR"+iMBR);
								            	System.err.println("intersecRes:"+intersecRes);
								            	
								            	vol = 0.0;
								            }

							            }
							        } catch(Exception e) {
							        	e.printStackTrace();
							        };
							    }
						
							
				        	 
				           
				        } catch(Exception e) {
				        	e.printStackTrace();
				        };
						
						
					}
					return list.iterator();
				}


				
			});
			resultRDD.count();
			

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
