package com.hqkang.SparkApp.core;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import com.hqkang.SparkApp.geom.Point;
import com.hqkang.SparkApp.geom.PointSet;

import scala.Tuple2;

public class PixelizationHelper {
	
	public static void Pixelate(JavaSparkContext sc, Point lb, Point rt, String fileName, int part,int wSeg,int hSeg,long timeIntervals, String outputPath) {

		JavaPairRDD<String, String> input = sc.wholeTextFiles(fileName, part);
		String currentPath = outputPath+"/"+System.currentTimeMillis()+"/";
		
		//System.out.println(input.count());
		//input.repartition(part);
		JavaPairRDD<String, PointSet> points = input
				.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, String>>, String, PointSet>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Iterator<Tuple2<String, PointSet>> call(Iterator<Tuple2<String, String>> s) {
						// TODO Auto-generated method stub
						ArrayList res = new ArrayList();
						// if(!t._1.endsWith(".plt"))
						// return res.iterator();
					while(s.hasNext())
					{
						Tuple2<String, String> t = s.next();
						String text = t._2;
						PointSet ls = new PointSet();
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
		
		JavaPairRDD<String,HashMap> resRDD = points.mapPartitionsToPair( new PairFlatMapFunction<Iterator<Tuple2<String, PointSet>>, String, HashMap>() {

			@Override
			public Iterator<Tuple2<String, HashMap>> call(Iterator<Tuple2<String, PointSet>> t) throws Exception {
				Double width = Math.abs(lb.X()-rt.X());
				Double height = Math.abs(lb.Y()-rt.Y());
				Double cellW = width/wSeg;
				Double cellH = height/hSeg;
				long seconds = Duration.ofDays(1).getSeconds();
				ArrayList<Tuple2<String, HashMap>> list = new ArrayList<Tuple2<String, HashMap>>();
				HashMap<Integer,Tuple2<Integer,Integer>> map = new HashMap<Integer,Tuple2<Integer,Integer>>();
				String key = "";
				while(t.hasNext()) {
					Tuple2 tu = t.next();
					key = (String) tu._1;
					PointSet trajectory = (PointSet) tu._2;
					String traName = (String) tu._1;
					long todayTime = (trajectory.gettSet().firstKey().getTime()/(1000*seconds))*seconds;
					for(long time = 0;time < seconds; time = time+timeIntervals) {
						Point currentPst = trajectory.getPtSnp((time+todayTime)*1000);
						int timeIndex = (int) (time/timeIntervals);

						
						if(null==currentPst) {
							map.put(timeIndex, new Tuple2(0,0));
							continue;
						}
						if(currentPst.X()<lb.X()|| currentPst.Y()< lb.Y() || currentPst.X() > rt.X() || currentPst.Y() > rt.Y()) {
							map.put(timeIndex,new Tuple2(0,0));
							continue;
						}
							
						int wIndex = (int) ((currentPst.X()-lb.X())/cellW);
						int hIndex = (int) ((currentPst.Y()-lb.Y())/cellH);
						map.put(timeIndex, new Tuple2(wIndex,hIndex));

						
					}
					list.add(new Tuple2(key,map));
				}
				
				// TODO Auto-generated method stub
				return list.iterator();
			}
			
		});
	
		JavaRDD<String> fileRDD = resRDD.flatMap(new FlatMapFunction<Tuple2<String, HashMap>, String>() {

			@Override
			public Iterator<String> call(Tuple2<String, HashMap> t) throws Exception {
				// TODO Auto-generated method stub
				String content = "";
				content += t._1+"\n";
				SortedSet<Integer> keys = new TreeSet<Integer>(t._2.keySet());
				for (Integer key : keys) {
					   Tuple2 val = (Tuple2) t._2.get(key);
					   
					   String value = (Integer) val._1 +","+ (Integer) val._2;
					   content += value + "\n";
					   // do something
					}
				
				ArrayList<String> list = new ArrayList<String>();
				list.add(content);
				return list.iterator();
				
			}
			
		});
		
		fileRDD.saveAsTextFile(currentPath);
		
	}

}
