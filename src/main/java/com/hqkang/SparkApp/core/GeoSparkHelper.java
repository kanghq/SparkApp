package com.hqkang.SparkApp.core;

import java.io.File;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.TreeMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ivy.util.MemoryUtil;
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
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
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

import com.hqkang.SparkApp.geom.MBR;
import com.hqkang.SparkApp.geom.MBRList;
import com.hqkang.SparkApp.geom.MBRRDDKey;
import com.hqkang.SparkApp.geom.Point;
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
import scala.Tuple3;

import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;;

public class GeoSparkHelper {


	public static JavaPairRDD<MBRRDDKey, MBR> toDBRDD(JavaPairRDD<String, MBRList> mbrRDD) {
		JavaPairRDD<MBRRDDKey, MBR> databaseRDD = mbrRDD
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, MBRList>, MBRRDDKey, MBR>() {
					public Iterator<Tuple2<MBRRDDKey, MBR>> call(Tuple2<String, MBRList> t) {
						Iterator<MBR> ite = t._2.iterator();
						int i = 0;
						List<Tuple2<MBRRDDKey, MBR>> list = new ArrayList<Tuple2<MBRRDDKey, MBR>>();
						try {
							while (ite.hasNext()) {

								MBR ele = ite.next();
								ele.setSeq(Integer.toString(i));
								ele.setTraID(t._1);
								MBRRDDKey idx = new MBRRDDKey(i, t._1);

								list.add(new Tuple2<MBRRDDKey, MBR>(idx, ele));
								i++;
							}

						} catch (Exception e) {
						}
						return list.iterator();

					}
				});
		
		return databaseRDD;
	}
	public static PolygonRDD transformToPolygonRDD(JavaPairRDD<MBRRDDKey, MBR> databaseRDD) {
		JavaRDD<Polygon> myPolygonRDD = databaseRDD
				.mapPartitions(new FlatMapFunction<Iterator<Tuple2<MBRRDDKey, MBR>>, Polygon>() {

					@Override
					public Iterator<Polygon> call(Iterator<Tuple2<MBRRDDKey, MBR>> t) throws Exception {
						// TODO Auto-generated method stub
						ArrayList<Polygon> list = new ArrayList<Polygon>();
						while (t.hasNext()) {
							Tuple2<MBRRDDKey, MBR> tu = t.next();
							MBR ele = tu._2;

							Polygon pol = ele.shape();
							String[] property = { "TraID", "Seq", "StartTime", "EndTime", "MBRJSON" };
							DecimalFormat df = new DecimalFormat("#");

							// String json =
							 //Gson.class.newInstance().toJson(ele);

							// String[] propertyField = { ele.getTraID(),
							// ele.getSeq(), df.format(ele.getTMin()),
							// df.format(ele.getTMax()), json };

							 //pol.setUserData(ele);
							 pol.setUserData(tu._1);
							list.add(pol);

						}
						return list.iterator();

					}
				});
		
		

		PolygonRDD geoPRDD = new PolygonRDD(myPolygonRDD, StorageLevel.MEMORY_ONLY());

		try {
			geoPRDD.spatialPartitioning(GridType.RTREE);
			geoPRDD.buildIndex(IndexType.RTREE, true);

			geoPRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY());
			geoPRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY());

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return geoPRDD;
	}

	public static JavaPairRDD<Tuple2, MBR> toTupleKey(JavaPairRDD<String, MBRList> mbrRDD) {
		JavaPairRDD<Tuple2, MBR> databaseRDD = mbrRDD
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, MBRList>, Tuple2, MBR>() {
					public Iterator<Tuple2<Tuple2, MBR>> call(Tuple2<String, MBRList> t) {
						Iterator<MBR> ite = t._2.iterator();
						int i = 0;
						List<Tuple2<Tuple2, MBR>> list = new ArrayList<Tuple2<Tuple2, MBR>>();
						while (ite.hasNext()) {

							MBR ele = ite.next();
							ele.setSeq(Integer.toString(i));
							ele.setTraID(t._1);
							Tuple2 idx = new Tuple2(i, t._1);
							list.add(new Tuple2(idx, ele));
							i++;
						}
						return list.iterator();

					}
				});
		return databaseRDD;
	}



	public static JavaPairRDD retrieve(PolygonRDD geoPRDD, boolean addAll, int stage,JavaPairRDD<MBRRDDKey, MBR> databaseRDD) {
		JavaPairRDD<Polygon, HashSet<Polygon>> joinedRDD = null;
		try {
			joinedRDD = JoinQuery.SpatialJoinQuery(geoPRDD, geoPRDD, true, true); //use index, consider !contains only?
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		JavaPairRDD<Polygon,Polygon> flattenedRDD = joinedRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Polygon,HashSet<Polygon>>>,Polygon,Polygon>() {

			@Override
			public Iterator<Tuple2<Polygon, Polygon>> call(Iterator<Tuple2<Polygon, HashSet<Polygon>>> s) {
				
				// TODO Auto-generated method stub
				ArrayList<Tuple2<Polygon, Polygon>> list = new ArrayList<Tuple2<Polygon, Polygon>>();
				while(s.hasNext())
				{
					Tuple2<Polygon, HashSet<Polygon>> t =s.next();
				Polygon key = t._1;
				
				for(Polygon val:t._2) {
					if(key.getCoordinates()[4].z>val.getCoordinates()[0].z&&key.getCoordinates()[0].z<val.getCoordinates()[4].z)
					list.add(new Tuple2(key,val));
				}
				
				}
				
				return list.iterator();
			}

		
			
		} );
		
		JavaPairRDD<MBRRDDKey,Tuple2<MBRRDDKey,MBRRDDKey>> st1RDD =  flattenedRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Polygon,Polygon>>,MBRRDDKey,Tuple2<MBRRDDKey,MBRRDDKey>>() {

			@Override
			public Iterator<Tuple2<MBRRDDKey, Tuple2<MBRRDDKey, MBRRDDKey>>> call(Iterator<Tuple2<Polygon, Polygon>> s)
					throws Exception {
				// TODO Auto-generated method stub
				
				ArrayList<Tuple2<MBRRDDKey,Tuple2<MBRRDDKey,MBRRDDKey>>> list = new 	ArrayList<Tuple2<MBRRDDKey,Tuple2<MBRRDDKey,MBRRDDKey>>>();
				while(s.hasNext())
				{
					Tuple2<Polygon, Polygon> t = s.next();
				
				list.add(new Tuple2((MBRRDDKey)t._2.getUserData(), new Tuple2<MBRRDDKey,MBRRDDKey>((MBRRDDKey)t._1.getUserData(),(MBRRDDKey)t._2.getUserData())));
			}
				return list.iterator();
			}


			
		});
		/*
		 * 
			<Polygon1,Polygon2>
			flatmaptopair
			<MBRRDD2<MBRRRDD1,MBRRDD2>>
			join
			<MBRRDD2<<MBRRDD1,MBRRDD2>,MBR2>>
			flatmaptopair
			<MBRRDD1,MBR2>
			join
			<MBRRDD1,<MBR2，MBR1>>
			values
			<MBR2,MBR1>
		 * */
		
		JavaPairRDD<MBRRDDKey,Tuple2<Tuple2<MBRRDDKey,MBRRDDKey>,MBR>> st2RDD = st1RDD.join(databaseRDD);
		JavaPairRDD<MBRRDDKey,MBR> st3RDD = st2RDD.flatMapToPair(new PairFlatMapFunction<Tuple2<MBRRDDKey,Tuple2<Tuple2<MBRRDDKey,MBRRDDKey>,MBR>>,MBRRDDKey,MBR>() {

			@Override
			public Iterator<Tuple2<MBRRDDKey, MBR>> call(Tuple2<MBRRDDKey, Tuple2<Tuple2<MBRRDDKey, MBRRDDKey>, MBR>> t)
					throws Exception {
				// TODO Auto-generated method stub
				ArrayList<Tuple2<MBRRDDKey,MBR>> list = new ArrayList<Tuple2<MBRRDDKey,MBR>>();
				list.add(new Tuple2(t._2._1._1,t._2._2));
				return list.iterator();
			}
			
		});
		JavaPairRDD<MBRRDDKey,Tuple2<MBR,MBR>> st4RDD = st3RDD.join(databaseRDD);
		JavaRDD<Tuple2<MBR,MBR>> st5RDD = st4RDD.values();

		

		JavaPairRDD<String, Tuple2> resultRDD = st5RDD
				.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<MBR,MBR>>,String,Tuple2>() {

					@Override
					public Iterator<Tuple2<String, Tuple2>> call(Iterator<Tuple2<MBR, MBR>> s) throws Exception {
																															// Auto-generated
																															// method
																															// stub
						ArrayList<Tuple2<String, Tuple2>> list = new ArrayList<Tuple2<String, Tuple2>>();
						/*
						Polygon queryPolygon = t._1;
						 JSONParser parser = new JSONParser();
						 String queryMBRJson = (String)
						 queryPolygon.getUserData();
						//MBR queryMBR = (MBR) queryPolygon.getUserData();
						 MBR queryMBR =
						 Gson.class.newInstance().fromJson(queryMBRJson,
						 MBR.class);

		
						//MBRRDDKey key = (MBRRDDKey) queryPolygon.getUserData();
						// MBR queryMBR = (MBR) (db.lookup((MBRRDDKey) key));
						 * */
						 while(s.hasNext())
						 {
							 Tuple2<MBR, MBR>	 t=s.next();
						 MBR queryMBR = t._1;
						 	//Polygon rs = t._2;
							 //String mbrjson = (String) rs.getUserData();
							 MBR iMBR = t._2;
							// Gson.class.newInstance().fromJson(mbrjson,
							 //MBR.class);
							Polygon section = null;
							Double vol = 0.0;
							Geometry intersecRes = null;
							try {
								Polygon queriedPol = iMBR.shape();
								String TraID = iMBR.getTraID();
								String Seq = iMBR.getSeq();
								Double startTime = iMBR.getTMin();
								Double endTime = iMBR.getTMax();
								Tuple2 resultMBR = new Tuple2(Seq, TraID);
								boolean collision = false;

								// System.out.println("Inters Obj"+queriedPol);
								if (startTime < queryMBR.getTMax() && endTime > queryMBR.getTMin()) {
									intersecRes = queriedPol.intersection(queryMBR.shape());

									section = (Polygon) intersecRes;
									// System.out.println("Inters NEW
									// obj"+section);
									vol = section.getArea();
								} else {
									vol = 0.0;
								}
								if (vol > 0.0) {
									Interval qInterval = new Interval(queryMBR.getTMin().longValue(),
											queryMBR.getTMax().longValue());
									Interval iInterval = new Interval(iMBR.getTMin().longValue(),
											iMBR.getTMax().longValue());
									Interval inters = qInterval.overlap(iInterval);
									Long intStart = inters.getStartMillis();
									Long intEnd = inters.getEndMillis();
									Long intMid = (intStart + intEnd) / 2;
									Point iStart = iMBR.getInsidePoints().getPtSnp(intStart);
									Point qStart = queryMBR.getInsidePoints().getPtSnp(intStart);
									Point iMid = iMBR.getInsidePoints().getPtSnp(intMid);
									Point qMid = queryMBR.getInsidePoints().getPtSnp(intMid);
									Point iEnd = iMBR.getInsidePoints().getPtSnp(intEnd);
									Point qEnd = queryMBR.getInsidePoints().getPtSnp(intEnd);
									Double disStart = iStart.distance(qStart);
									Double disMid = iMid.distance(qMid);
									Double disEnd = iEnd.distance(qEnd);

									if (disStart != 0 && disMid != 0 && disEnd != 0
											&& (disStart < 5 || disMid < 5 || disEnd < 5))
										collision = true;

								}
								if (true == addAll) {
									list.add(new Tuple2("QT:" + queryMBR.getTraID() + "," + TraID,
											new Tuple2(vol, new Tuple2(resultMBR, collision))));

								} else if (collision) {
									list.add(new Tuple2("QT:" + queryMBR.getTraID() + "," + TraID,
											new Tuple2(vol, new Tuple2(resultMBR, collision))));
								}

							} catch (ClassCastException | IllegalArgumentException e) {

								System.err.println("queriedMBR" + iMBR);
								System.err.println("intersecRes:" + intersecRes);

								vol = 0.0;
							}

						

					}
							return list.iterator();


					}

			

				});

		// resultRDD.count();

		JavaPairRDD<String, Tuple2<Double, Boolean>> canRDD = resultRDD.aggregateByKey(
				new Tuple2(new Double(0.0), new Boolean(false)),
				new Function2<Tuple2<Double, Boolean>, Tuple2, Tuple2<Double, Boolean>>() {

					public Tuple2<Double, Boolean> call(Tuple2<Double, Boolean> v1, Tuple2 v2) throws Exception {
						// TODO Auto-generated method stub
						Tuple2<Double, Tuple2> newv2 = (Tuple2<Double, Tuple2>) v2;
						Double val = v1._1;
						Boolean bval = v1._2;
						val = (Double) v2._1 + val;
						bval = bval || (Boolean) newv2._2._2;
						return new Tuple2<Double, Boolean>(val, bval);
					}

				}, new Function2<Tuple2<Double, Boolean>, Tuple2<Double, Boolean>, Tuple2<Double, Boolean>>() {

					@Override
					public Tuple2<Double, Boolean> call(Tuple2<Double, Boolean> v1, Tuple2<Double, Boolean> v2)
							throws Exception {
						// TODO Auto-generated method stub
						Double val = v1._1;
						Double val2 = v2._1;
						Boolean bval = v1._2;
						Boolean bval2 = v2._2;
						val = val + val2;
						bval = bval || bval2;
						return new Tuple2<Double, Boolean>(val, bval);
					}

				});

		/*
		 * canRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<Double,
		 * Boolean>>>() {
		 * 
		 * @Override public void call(Tuple2<String, Tuple2<Double, Boolean>> t)
		 * throws Exception { // TODO Auto-generated method stub
		 * System.out.println(t._1 + "," + t._2._1 + "," + t._2._2);
		 * 
		 * } });
		 */

		if (1 == stage) {
			return joinedRDD;
		}
		if (2 == stage) {
			return resultRDD;
		}
		return canRDD;
	}

	public static RowMatrix PCA(JavaPairRDD<String, Tuple2<Double, Boolean>> src, int num, JavaSparkContext sc,
			List<File> file) {

		Iterator<File> ite = file.iterator();
		TreeMap<String, Long> idx = new TreeMap<String, Long>();
		while (ite.hasNext()) {
			String keyentry = "file:" + ite.next().getAbsolutePath().toString();

			idx.put(keyentry, -1L);
		}
		String[] mapKeys = new String[idx.size()];
		int pos = 0;
		for (String keyEn : idx.keySet()) {
			mapKeys[pos++] = keyEn;
		}

		final Broadcast<String[]> fileList = sc.broadcast(mapKeys);

		JavaRDD<MatrixEntry> entryRDD = src
				.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String, Tuple2<Double, Boolean>>>, MatrixEntry>() {

					@Override
					public Iterator<MatrixEntry> call(Iterator<Tuple2<String, Tuple2<Double, Boolean>>> arg0)
							throws Exception {
						// TODO Auto-generated method stub

						String[] rcvd = fileList.getValue();

						ArrayList<MatrixEntry> entries = new ArrayList<MatrixEntry>();
						while (arg0.hasNext()) {
							Tuple2<String, Tuple2<Double, Boolean>> entry = arg0.next();
							double val = entry._2._1;
							String[] coo = entry._1.toString().substring(3).split(",");
							long row = Arrays.asList(rcvd).indexOf(coo[0]);
							long col = Arrays.asList(rcvd).indexOf(coo[1]);
							entries.add(new MatrixEntry(row, col, val));
						}

						return entries.iterator();
					}

				});

		CoordinateMatrix mat = new CoordinateMatrix(entryRDD.rdd());
		RowMatrix rMat = mat.toRowMatrix();
		Matrix pc = (Matrix) rMat.computePrincipalComponents(1);
		RowMatrix projeced = rMat.multiply(pc);

		return projeced;

	}
	
	public static void printResults(Layer layer, List<SpatialDatabaseRecord> results) {
		System.out.println("\tTesting layer '" + layer.getName() + "' (class " + layer.getClass() + "), found results: "
				+ results.size());
		for (SpatialDatabaseRecord r : results) {
			System.out.println("\t\tGeometry: " + r);
			Geometry geo = r.getGeometry();
			System.out.println(geo);
		}
	}

}
