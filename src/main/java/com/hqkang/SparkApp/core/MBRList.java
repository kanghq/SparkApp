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
			ele.merge(this.get(index+1));
			remove(index+1);
			return true;
		}
		return false;
		
	}
	
	TreeMap calculatePotentialMergeCost() {
		TreeMap<Double, Integer> res = new TreeMap();
		double thrVol = 9999;
		int nodeN = -1;
		for(int i = 0; i < size()-1; i++) {
			MBR ori = get(i);
			MBR sec = get(i+1);
			double volum = MBR.calculateVolume(ori, sec)-ori.volume();
			if(volum < thrVol) {
				thrVol = volum;
				nodeN = i;
			}
			
			
		}
		res.put(thrVol, nodeN);
		return res;
		
	}
	
	public static Tuple2<String, MBRList> segmentationFromPoints(int segs, Tuple2<String, LinkedList<Point>> t) {
		int k = segs;
		MBRList mbrPriList = new MBRList();
	
		String fileNmae = t._1();
		Collections.sort(t._2);
	
		Iterator ite = t._2().iterator();
		
		while(ite.hasNext()) {
			Point point = (Point)ite.next();
			mbrPriList.add(new MBR(point, point));
		}
		
		while(mbrPriList.size() > k) {
			TreeMap<Double, Integer> priQueue = mbrPriList.calculatePotentialMergeCost();
			mbrPriList.mergeNextOne(priQueue.firstEntry().getValue());
		}
		int size = mbrPriList.size();
		Collections.sort(mbrPriList);
		Iterator<MBR> mbrIte = mbrPriList.iterator();
		MBR first = mbrIte.next();
		if(first.getInsidePoints().size() == 1){
			MBR lastOne = null;

			
			while(mbrIte.hasNext()) {
				
				MBR sec = mbrIte.next();
				first.add(sec.getInsidePoints().first());
				lastOne = first;
				first = sec;
					
			}
			if(!mbrIte.hasNext()) {
				first.add(lastOne.getInsidePoints().last());
				if(first.getInsidePoints().size()==1) {
					mbrPriList.remove(first);
				}
			}
		}
		else {
			while(mbrIte.hasNext()) {
				
				MBR sec = mbrIte.next();
				sec.add(first.getInsidePoints().last());
				first = sec;
			}
		}
		
		
		
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
