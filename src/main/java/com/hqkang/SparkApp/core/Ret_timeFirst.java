package com.hqkang.SparkApp.core;



import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;

import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.StatCounter;
import org.neo4j.gis.spatial.SpatialDatabaseRecord;
import org.neo4j.gis.spatial.pipes.GeoPipeline;
import org.neo4j.gis.spatial.pipes.impl.FilterPipe;
import org.neo4j.gis.spatial.rtree.filter.SearchAll;
import org.neo4j.graphdb.Transaction;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;

import com.vividsolutions.jts.geom.Polygon;

import scala.Tuple2;

public class Ret_timeFirst {
	
	
	public static void main(String[] args) {
		
		// TODO Auto-generated method stub
		// Create a Java Spark Context
		
		SparkSession spark = SparkSession.builder().appName("wordCount").getOrCreate();
		//spark.conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		//spark.conf().set("spark.kryo.registrator", "MyRegistrator");
		JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
		// Load our input data.
		ResourceBundle rb = ResourceBundle.getBundle("Config");
		int k =20;
		String queryFile = "20081024020959.plt";
		try{
		    queryFile = rb.getString("queryFile");
			k  = Integer.parseInt(rb.getString("k"));

		}
		catch(MissingResourceException ex){}
	
	
		JavaPairRDD<String, MBRList> queRDD =  Helper.importFromFile(queryFile, sc, k);
		
		JavaPairRDD<Tuple2, MBR> queryRDD = Helper.toTupleKey(queRDD);
		
	
      
		JavaPairRDD<String, Tuple2> resultRDD = queryRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Tuple2, MBR>, String, Tuple2>() {



			@Override
			public Iterator<Tuple2<String, Tuple2>> call(Tuple2<Tuple2, MBR> t) throws Exception {
				// TODO Auto-generated method stub
				MBR queryMBR = t._2;
	            ArrayList<Tuple2<String, Tuple2>> list = new ArrayList<Tuple2<String, Tuple2>>();

				try (Transaction tx = new Neo4JCon().getDb().beginTx()) {
		        	List<SpatialDatabaseRecord> results = null;
		        	Envelope env = new Envelope(queryMBR.getXMin(), queryMBR.getXMax(), queryMBR.getYMin(), queryMBR.getYMax());
		        	GeoPipeline pip = GeoPipeline.start(new Neo4JCon().getLayer(), new SearchPropertyGeo("StartTime", queryMBR.getTMax(), "LESS_THAN") ).copyDatabaseRecordProperties().propertyFilter("EndTime", queryMBR.getTMin(), FilterPipe.Filter.GREATER_THAN).intersect(new Neo4JCon().getLayer().getGeometryFactory().toGeometry(env));
		        	//GeoPipeline pip = GeoPipeline
		         //           .startIntersectSearch(new Neo4JCon().getLayer(), new Neo4JCon().getLayer().getGeometryFactory().toGeometry(env)).propertyFilter("Seq", "0")
		          //         ;
		        	
		        //	GeoPipeline pip = GeoPipeline.start(new Neo4JCon().getLayer(), new SearchAll()).copyDatabaseRecordProperties().propertyFilter("Seq", "1");
		        	try{
		        	results = pip
		                    .toSpatialDatabaseRecordList();
		        	} catch(Exception e) {
		        		e.printStackTrace();
		        	}
		            for ( SpatialDatabaseRecord r : results )
			        {
			            System.out.println( "\t\tGeometry: " + r );
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
				            
				            System.out.println("Inters Obj"+queriedPol);
				            if(startTime< queryMBR.getTMax() && endTime > queryMBR.getTMin()) {
					            intersecRes = queriedPol.intersection(new Neo4JCon().getLayer().getGeometryFactory().createPolygon(queryMBR.shape()));
					            
				            	section = (Polygon) intersecRes;
					            System.out.println("Inters NEW obj"+section);
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
        
        canRDD.count();
		
		sc.stop();
	
	}

	private static ArrayList<File> ReadAllFiles() {
		// TODO Auto-generated method stub
		return null;
	}
  

}