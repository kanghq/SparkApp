package com.hqkang.SparkApp.core;

import java.util.LinkedList;

public class MBR {
	
	private Point lt;
	private Point rb;
	private LinkedList<Point> insidePoints;
	
	public MBR(Point _lt, Point _rb) {
		if(_lt.getX() > rb.getX()) {
			lt = _lt;
			rb = _rb;
		} else {
			lt = _rb;
			rb = _lt;
		}
	}
	
	public boolean setLT(Point _lt) {
		lt = _lt;
		return true;
	}
	
	public Point getLT() {
		return lt;
	}
	
	public boolean setRB(Point _rb) {
		rb = _rb;
		return true;
		
	}
	
	public Point getRB() {
		return rb;
	}
	
	
	public double getVolume() {
		return Math.abs((rb.getX()-lt.getX())*(lt.getY()-rb.getY()));
	}

	public static double calculateVolume(Point _lt, Point _rb) {
		return Math.abs((_rb.getX()-_lt.getX())*(_lt.getY()-_rb.getY()));
	}
}
