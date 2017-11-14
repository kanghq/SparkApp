package com.hqkang.SparkApp.core;





import java.io.File;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.joda.time.Interval;

import com.hqkang.SparkApp.cli.GeoSparkParser;
import com.hqkang.SparkApp.cli.SubmitParser;
import com.hqkang.SparkApp.geom.MBR;
import com.hqkang.SparkApp.geom.MBRList;
import com.hqkang.SparkApp.geom.MBRRDDKey;
import com.hqkang.SparkApp.geom.Point;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;

import scala.Tuple2;

public class duplicate_test {

	public static void main(String[] args) {
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
		JavaPairRDD<String, String> input = sc.wholeTextFiles(filePath, part);
		
		//System.out.println(input.count());
		//input.repartition(part);
		JavaPairRDD<String, LinkedList<Point>> points = input
				.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, String>>, String, LinkedList<Point>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<Tuple2<String, LinkedList<Point>>> call(Iterator<Tuple2<String, String>> s) {
						// TODO Auto-generated method stub
						ArrayList res = new ArrayList();
						// if(!t._1.endsWith(".plt"))
						// return res.iterator();
					while(s.hasNext())
					{
						Tuple2<String, String> t = s.next();
						String text = t._2;
						LinkedList<Point> ls = new LinkedList<Point>();
						String line[] = text.split("\n");
						for (int i = 6; i < line.length; i++) {

							try {
								String[] parts = line[i].split(",");
								String lat = parts[0];
								String lon = parts[1];
								String sDate = parts[5];
								Date date;
								String sTime = parts[6];
								Date time;
								Point pt = new Point(sDate, sTime, lat, lon);
								if (pt.getTime() == null) {
									System.err.println("Error Point" + pt);
								}
								ls.add(pt);

							} catch (Exception e) {

							}
						}
						res.add(new Tuple2(t._1, ls));
						
					}return res.iterator();

					}

	

				});
		points.cache();
		points.count();
		points.repartition(2*part);

		//Partitioner p = new HashPartitioner(part);
		//points.partitionBy(p);

		//points.count();f

		JavaPairRDD<String, MBRList> mbrRDD = points
				.mapToPair(new PairFunction<Tuple2<String, LinkedList<Point>>, String, MBRList>() {

					public Tuple2<String, MBRList> call(Tuple2<String, LinkedList<Point>> t) throws Exception {
						return MBRList.segmentationFromPoints(k, t);
					}

				});
		mbrRDD.cache();
		mbrRDD.count();
		
		JavaPairRDD<MBRRDDKey, MBR> dbrdd = mbrRDD
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
								MBRRDDKey idx = new MBRRDDKey(t._1, i);

								list.add(new Tuple2<MBRRDDKey, MBR>(idx, ele));
								i++;
							}

						} catch (Exception e) {
						}
						return list.iterator();

					}
				});
	
		dbrdd.cache();
		dbrdd.count();
		JavaRDD<Polygon> myPolygonRDD = dbrdd
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
		
		myPolygonRDD.cache();
		myPolygonRDD.count();

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
		

		// databaseRDD.count();
		/*
		 * mypolygonRDD.foreach(new VoidFunction<Polygon>() {
		 * 
		 * @Override public void call(Polygon t) throws Exception { // TODO
		 * Auto-generated method stub System.out.println(t); }
		 * 
		 * });
		 */
		geoPRDD.indexedRDD.count();
		geoPRDD.spatialPartitionedRDD.count();
		JavaPairRDD<Polygon, HashSet<Polygon>> joinedRDD = null;
		try {
			//joinedRDD = JoinQuery.SpatialJoinQuery(geoPRDD, geoPRDD, false, true); //use index, consider !contains only?

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
		st1RDD.count();
		st1RDD.cache();
		st1RDD.count();
		st1RDD.count();

		JavaPairRDD<MBRRDDKey,Tuple2<Tuple2<MBRRDDKey,MBRRDDKey>,MBR>> st2RDD = st1RDD.join(dbrdd);
		st2RDD.count();
		st2RDD.cache();
		st2RDD.count();
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
		st3RDD.count();
		st3RDD.cache();
		st3RDD.count();
		JavaPairRDD<MBRRDDKey,Tuple2<MBR,MBR>> st4RDD = st3RDD.join(dbrdd);
		st4RDD.cache();
		st4RDD.count();
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
								boolean addAll = true;
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
		resultRDD.saveAsTextFile(outputPath+System.currentTimeMillis());
		sc.stop();

	}


}
