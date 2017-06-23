package com.hqkang.SparkApp.core;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import com.hqkang.SparkApp.geom.MBR;
import com.hqkang.SparkApp.geom.MBRList;
import com.hqkang.SparkApp.geom.Point;

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

		JavaPairRDD<String, String> input = sc.wholeTextFiles(fileName,part);
		
		
		//System.out.println(input.count());
		JavaPairRDD<String, LinkedList<Point>> points = input
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, String>, String, LinkedList<Point>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<Tuple2<String, LinkedList<Point>>> call(Tuple2<String, String> t) throws Exception {
						// TODO Auto-generated method stub
						ArrayList res = new ArrayList();
						// if(!t._1.endsWith(".plt"))
						// return res.iterator();
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
						return res.iterator();

					}

				});
		points.cache();

		Partitioner p = new HashPartitioner(part);
		points.partitionBy(p);

		//points.count();

		JavaPairRDD<String, MBRList> mbrRDD = points
				.mapToPair(new PairFunction<Tuple2<String, LinkedList<Point>>, String, MBRList>() {

					public Tuple2<String, MBRList> call(Tuple2<String, LinkedList<Point>> t) throws Exception {
						return MBRList.segmentationFromPoints(k, t);
					}

				}).cache();
		//mbrRDD.count();
		return mbrRDD;

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
				}).cache();
		return databaseRDD;
	}

}
