package com.hqkang.SparkApp.core;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import com.hqkang.SparkApp.core.MBR;

public class MBRList extends java.util.LinkedList<MBR> {
	
	boolean mergeNextOne(int index) {
		
		MBR ele =  get(index);
		MBR secE = get(index+1);
		
		if(null != secE)
		{
			ele.setRB(secE.getRB());
			remove(index+1);
			return true;
		}
		return false;
		
	}
	
	TreeMap calculatePotentialMergeCost() {
		TreeMap<Integer, Double> res = new TreeMap();
		for(int i = 0; i < size()-1; i++) {
			MBR ori = get(i);
			MBR sec = get(i+1);
			double volum = MBR.calculateVolume(ori.getLT(), sec.getRB())-ori.getVolume();
			res.put(i, volum);
			
		}
		return res;
		
	}

}
