package com.hqkang.SparkApp.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeMap;

public class MBR implements Comparable,Serializable{
	

	private Point LT;
	private Point RT;
	private Point LB;
	private Point RB;
	private PointSet insidePoints = new PointSet();
	private String traID;
	private String seq;

	
	public MBR(Point _lt, Point _rb) {
		
		
		this.insidePoints.add(_lt);
		this.insidePoints.add(_rb);

		Iterator<Point> ite =	this.insidePoints.range().iterator();
		LT = (Point) ite.next();
		RT = (Point) ite.next();
		RB = (Point) ite.next();
		LB = (Point) ite.next();

		
	}

	public void setLT(Point _lt) {
		LT = _lt;
	}
	
	
	public Point getLT() {
		return LT;
	}
	
	public void setRB(Point _rb) {
		RB = _rb;
		
	}
	
	
	public Point getRB() {
		return RB;
	}
	
	
	public double volume() {
		return Math.abs((RB.X()-LT.X())*(LT.Y()-RB.Y()));
	}

	public static double calculateVolume(MBR first, MBR sec) {
		PointSet portential = new PointSet();
		portential.addAll(sec.insidePoints);
		portential.addAll(first.insidePoints);
		ArrayList<Point> ptList = portential.range();
		Point _lt = ptList.get(0);
		Point _rb = ptList.get(2);
		return Math.abs((_rb.X()-_lt.X())*(_lt.Y()-_rb.Y()));
	}
	
	public boolean merge(MBR m) {
		this.insidePoints.addAll(m.insidePoints);
		
		return true;
	}

	

	
	
	public ArrayList<Point> convert2Polygen() {
		return this.insidePoints.range();
		
	}

	@Override
	public int compareTo(Object sec) {
		MBR seco= (MBR)sec;
		
		
		if(this.insidePoints.first().getTime().after(seco.insidePoints.last().getTime())) {
			return 1;
		}
		else return -1;
		// TODO Auto-generated method stub
	}
	
	public void setTraID(String traID) {
		this.traID = traID;
	}
	
	public String getTraID() {
		return this.traID;
	}
	
	public void setSeq(String seq) {
		this.seq = seq;
	}
	
	public String toString() {
		return this.traID+ " _ " + this.seq+" " + this.LT + this.insidePoints.first().getTime()+" " + this.RB +" " + this.LT + this.insidePoints.last().getTime()+" "+ " total:"+ this.insidePoints.size()+ " \n";
	}

	public Point getRT() {
		return RT;
	}

	public void setRT(Point rT) {
		RT = rT;
	}

	public Point getLB() {
		return LB;
	}

	public void setLB(Point lB) {
		LB = lB;
	}

	public PointSet getInsidePoints() {
		return insidePoints;
	}

	public void setInsidePoints(PointSet insidePoints) {
		this.insidePoints = insidePoints;
	}

	public String getSeq() {
		return seq;
	}
	


}
