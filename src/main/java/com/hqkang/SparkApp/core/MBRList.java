package com.hqkang.SparkApp.core;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.TreeMap;

import com.hqkang.SparkApp.core.MBR;

import scala.Tuple2;

public class MBRList extends java.util.LinkedList<MBR>  {
	
	boolean mergeNextOne(int index) {
		
		MBR ele =  get(index);
		MBR secE = get(index+1);
		
		if(null != secE)
		{
			ele.setRB(secE.getRB());
			ele.merge(this.get(index+1));
			remove(index+1);
			return true;
		}
		return false;
		
	}
	
	TreeMap calculatePotentialMergeCost() {
		TreeMap<Double, Integer> res = new TreeMap();
		for(int i = 0; i < size()-1; i++) {
			MBR ori = get(i);
			MBR sec = get(i+1);
			double volum = MBR.calculateVolume(ori, sec)-ori.volume();
			res.put(volum, i);
			
		}
		return res;
		
	}
	
	public static Tuple2<String, MBRList> segmentationFromPoints(int segs, Tuple2<String, LinkedList<Point>> t) {
		int k = segs;
		MBRList mbrPriList = new MBRList();
	
		String fileNmae = t._1();
	
		Iterator ite = t._2().iterator();
		
		while(ite.hasNext()) {
			Point point = (Point)ite.next();
			mbrPriList.add(new MBR(point, point));
		}
		
		while(mbrPriList.size() > k) {
		TreeMap<Double, Integer> priQueue = mbrPriList.calculatePotentialMergeCost();
		mbrPriList.mergeNextOne(priQueue.firstEntry().getValue());
		}
		Collections.sort(mbrPriList);
		
		return new Tuple2(fileNmae, mbrPriList);
		
		
	}
	public String toString() {
		String ctx ="";
		for(int i = 0;i< this.size();i++) {
			ctx += this.get(i).toString();
		}
		
		return ctx;
				
				
				
	}

}
