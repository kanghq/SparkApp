package com.hqkang.SparkApp.geom;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import com.hqkang.SparkApp.geom.MBR;

import scala.Tuple2;

public class MBRList extends java.util.LinkedList<MBR>  {
	HashMap<MBRKey, Double> lookup = new HashMap<MBRKey, Double>();
	
	boolean mergeNextOne(int index) {
		
		MBR ele =  get(index);
		MBR secE = get(index+1);
		
		if(null != secE)
		{
			ele.merge(this.get(index+1));
			remove(index+1);
			if(index+1<this.size()){			//index+1=58 = size ,index=57
				ele.setMergeNextCost(MBR.calculateVolume(ele, get(index+1)));
			} else {
				get(index).setMergeNextCost(new Double(-2));
			}
			if(index-1>=0) {
				MBR beforeEle = get(index-1);
				beforeEle.setMergeNextCost(MBR.calculateVolume(beforeEle, ele));
			}
			return true;
		}
		return false;
		
	}
	
	
	static int leastMergeCost(MBRList list) {
		ArrayList<Double> cost = new ArrayList<Double>();
		for(MBR ele: list) {
			double costVal = ele.getMergeNextCost();
			if(costVal!=-2)
			cost.add(costVal);
			
		}
		Double min = Collections.min(cost);
		int index = cost.indexOf(min);
		if(list.size()-1 <= index) {
			list.get(list.size()-1).setMergeNextCost(new Double(Double.POSITIVE_INFINITY));
			return leastMergeCost(list);
		}
		return index;
	}
	
	HashMap calculatePotentialMergeCost() {
		HashMap<Double, Integer> res = new HashMap<Double, Integer>();
		double thrVol = 9999;
		int nodeN = -1;
		for(int i = 0; i < size()-1; i++) {
			MBR ori = get(i);
			MBR sec = get(i+1);
			double volum = -1;
			MBRKey lookupKey = new MBRKey(ori, sec);

			if(lookup.containsKey(lookupKey))
			{
				volum = lookup.get(lookupKey);
			} else {
				volum = MBR.calculateVolume(ori, sec)-ori.getVolume();
				lookup.put(lookupKey, volum);
			}
			
			if(volum < thrVol) {
				thrVol = volum;
				nodeN = i;
			}
			
			
		}
		res.put(thrVol, nodeN);
		return res;
		
	}
	 public boolean add(MBR e) {
	        boolean res = super.add(e);
	        int lastIndex = lastIndexOf(e);
	        if(lastIndex-1>=0) {
		        MBR beforeLast = get(lastIndex-1);
		        beforeLast.setMergeNextCost(MBR.calculateVolume(beforeLast, e));
	        }
	        return res;
	    }
	
	public static Tuple2<String, MBRList> segmentationFromPoints(int segs, Tuple2<String, LinkedList<Point>> t) {
		int k = segs;
		MBRList mbrPriList = new MBRList();
	
		String fileNmae = t._1();
		Collections.sort(t._2);
	
		Iterator ite = t._2().iterator();
		Point preP = null;
		if(ite.hasNext())
			preP = (Point)ite.next();
			
		
		while(ite.hasNext()) {
			Point point = (Point)ite.next();
			mbrPriList.add(new MBR(preP, point));
			preP = point;
		}
		
		while(mbrPriList.size() > k) {
			int key = MBRList.leastMergeCost(mbrPriList);
			
			mbrPriList.mergeNextOne(key);
		}
		int size = mbrPriList.size();
		Collections.sort(mbrPriList);
		Iterator<MBR> mbrIte = mbrPriList.iterator();
		MBR first = mbrIte.next();
		/*
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
		
		*/
		
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
