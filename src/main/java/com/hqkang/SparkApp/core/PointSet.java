package com.hqkang.SparkApp.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.TreeMap;

public class PointSet extends LinkedHashSet<Point> implements Serializable{
	TreeMap<Double, Point> xSet = new TreeMap<Double, Point>();
	TreeMap<Double, Point> ySet = new TreeMap<Double, Point>();
	TreeMap<Date, Point> tSet = new TreeMap<Date, Point>();

	
	public boolean add(Point pt) {
		boolean res = super.add(pt);
		
		xSet.put(pt.X(), pt);
		ySet.put(pt.Y(), pt);
		tSet.put(pt.T(), pt);
		return res;
	}
	
	public boolean addAll(Collection <? extends Point> c) {
		
		boolean res = super.addAll(c);
		Iterator<? extends Point> ite = c.iterator();
		while(ite.hasNext()) {
			Point p = ite.next();
			xSet.put(p.X(),p);
			ySet.put(p.Y(), p);
			tSet.put(p.T(), p);

		}
		
		
		return res;
		
	}
	
	public Point first() {
		Point[] arr = this.toArray(new Point[this.size()]);
		return arr[0];
	}
	
	public Point last() {
		Point[] arr = 
		this.toArray(new Point[this.size()]);
		return arr[this.size()-1];
	}
	
	
	

	public boolean remove(Point pt) {
		boolean res = super.remove(pt);
		xSet.remove(pt.X(), pt);
		ySet.remove(pt.Y(), pt);
		tSet.remove(pt.T(), pt);
		return res;
	}
	
	public ArrayList<Double> range() {
		double XMin = xSet.firstKey();
		double XMax = xSet.lastKey();
		double YMax = ySet.lastKey();
		double YMin = ySet.firstKey();
		double TMin = tSet.firstKey().getTime();
		double TMax = tSet.lastKey().getTime();
		
		ArrayList<Double> res = new ArrayList<Double>();
		res.add(XMin);
		res.add(XMax);
		res.add(YMin);
		res.add(YMax);
		res.add(TMin);
		res.add(TMax);
		return res;
		
	}

	public TreeMap<Double, Point> getxSet() {
		return xSet;
	}

	public void setxSet(TreeMap<Double, Point> xSet) {
		this.xSet = xSet;
	}

	public TreeMap<Double, Point> getySet() {
		return ySet;
	}

	public void setySet(TreeMap<Double, Point> ySet) {
		this.ySet = ySet;
	}
	
	
}
