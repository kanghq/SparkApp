package com.hqkang.SparkApp.core;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.spatialRDD.LineStringRDD;
import org.neo4j.driver.v1.exceptions.ClientException;

import com.hqkang.SparkApp.geom.MBR;
import com.hqkang.SparkApp.geom.MBRList;
import com.hqkang.SparkApp.geom.Point;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;

import scala.Tuple2;

public class CommonHelper {

	public static ArrayList<File> ReadAllFile(String filePath) {
		File f = null;
		f = new File(filePath);
		File[] files = f.listFiles(); // get all files from f folder
		ArrayList<File> list = new ArrayList<File>();
		for (File file : files) {
			if (file.isDirectory()) {
				// if file is a directory，read its files recursively
				ReadAllFile(file.getAbsolutePath());
			} else {
				if (file.getName().endsWith("plt")) {
					list.add(file);
				}
			}
		}

		return list;

	}

	public static String conString(String[] list) {
		String str = "[";
		for (int i = 0; i < list.length; i++) {

			str += "'";
			str += list[i].toString();
			str += "',";

		}
		if (str.endsWith(",")) {
			str = str.substring(0, str.length() - 1) + "]";
		}
		return str;

	}

	public static JavaPairRDD<String, MBRList> importFromFile(String fileName, JavaSparkContext sc, int k, int part) {

		JavaPairRDD<String, String> input = sc.wholeTextFiles(fileName, part);

		// System.out.println(input.count());
		// input.repartition(part);
		JavaPairRDD<String, LinkedList<Point>> points = input.mapPartitionsToPair(
				new PairFlatMapFunction<Iterator<Tuple2<String, String>>, String, LinkedList<Point>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<Tuple2<String, LinkedList<Point>>> call(Iterator<Tuple2<String, String>> s) {
						// TODO Auto-generated method stub
						ArrayList res = new ArrayList();

						try {
							//Connection con = DriverManager.getConnection("jdbc:neo4j:bolt://" + "localhost", "spark","25519173");
							String unique = "create constraint on (n:Trajectory) assert nid is unique" ;

							// if(!t._1.endsWith(".plt"))
							// return res.iterator();
							while (s.hasNext()) {
								Tuple2<String, String> t = s.next();
								String text = t._2;
								String traID = t._1.substring(t._1.lastIndexOf("/")+1);
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
								
								String query = "merge (n:Trajectory{ID:\""+traID +"\"})";
								//DBHelper.retry(0, 3, con, query);

							}
							
							

						} catch (ClientException e2) {
							e2.printStackTrace();
						}

						return res.iterator();

					}

				});

		// Partitioner p = new HashPartitioner(part);
		// points.partitionBy(p);

		// points.count();f

		JavaPairRDD<String, MBRList> mbrRDD = points
				.mapToPair(new PairFunction<Tuple2<String, LinkedList<Point>>, String, MBRList>() {

					public Tuple2<String, MBRList> call(Tuple2<String, LinkedList<Point>> t) throws Exception {
						return MBRList.segmentationFromPoints(k, t);
					}

				});
		// mbrRDD.count();
		return mbrRDD;

	}

	/*
	 * import trajector and output as line string mode
	 */
	public static LineStringRDD importLSTra(String fileName, JavaSparkContext sc, int k, int part) {

		JavaPairRDD<String, String> input = sc.wholeTextFiles(fileName, part);

		// System.out.println(input.count());
		// input.repartition(part);
		JavaPairRDD<String, LinkedList<Coordinate>> points = input.mapPartitionsToPair(
				new PairFlatMapFunction<Iterator<Tuple2<String, String>>, String, LinkedList<Coordinate>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<Tuple2<String, LinkedList<Coordinate>>> call(Iterator<Tuple2<String, String>> s) {
						// TODO Auto-generated method stub
						ArrayList res = new ArrayList();
						// if(!t._1.endsWith(".plt"))
						// return res.iterator();
						while (s.hasNext()) {
							Tuple2<String, String> t = s.next();
							String text = t._2;
							LinkedList<Coordinate> ls = new LinkedList<Coordinate>();
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

									Coordinate pt = new Coordinate(Double.parseDouble(lon), Double.parseDouble(lat));

									ls.add(pt);

								} catch (Exception e) {

								}
							}
							res.add(new Tuple2(t._1, ls));

						}
						return res.iterator();

					}

				});
		JavaPairRDD<String, LineString> LSRDD = points.mapPartitionsToPair(
				new PairFlatMapFunction<Iterator<Tuple2<String, LinkedList<Coordinate>>>, String, LineString>() {

					@Override
					public Iterator<Tuple2<String, LineString>> call(Iterator<Tuple2<String, LinkedList<Coordinate>>> t)
							throws Exception {
						// TODO Auto-generated method stub
						ArrayList<Tuple2<String, LineString>> list = new ArrayList<Tuple2<String, LineString>>();
						while (t.hasNext()) {
							Tuple2 s = t.next();
							LinkedList<Coordinate> lst = (LinkedList<Coordinate>) s._2;
							LineString ls = new GeometryFactory()
									.createLineString(lst.toArray(new Coordinate[lst.size()]));
							list.add(new Tuple2(s._1, ls));
						}
						return list.iterator();
					}

				});
		LineStringRDD lsrdd = new LineStringRDD(LSRDD.values(), StorageLevel.MEMORY_ONLY_SER());

		return lsrdd;

	}

	public static JavaPairRDD<Tuple2, MBR> toTupleKey(JavaPairRDD<String, MBRList> mbrRDD) {
		JavaPairRDD<Tuple2, MBR> databaseRDD = mbrRDD
				.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, MBRList>>, Tuple2, MBR>() {
					public Iterator<Tuple2<Tuple2, MBR>> call(Iterator<Tuple2<String, MBRList>> s) throws Exception {
						List<Tuple2<Tuple2, MBR>> list = new ArrayList<Tuple2<Tuple2, MBR>>();

						while (s.hasNext()) {
							Tuple2<String, MBRList> t = s.next();
							Iterator<MBR> ite = t._2.iterator();
							int i = 0;
							while (ite.hasNext()) {

								MBR ele = ite.next();
								ele.setSeq(Integer.toString(i));
								ele.setTraID(t._1);
								Tuple2 idx = new Tuple2(i, t._1);
								list.add(new Tuple2(idx, ele));
								i++;
							}
						}
						return list.iterator();

					}

				});
		return databaseRDD;
	}

}
