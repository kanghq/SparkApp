package com.hqkang.SparkApp.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeMap;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;

public class MBR implements Comparable,Serializable{
	


	private PointSet insidePoints = new PointSet();
	private String traID;
	private String seq;

	
	public MBR(Point _lt, Point _rb) {
		
		this.insidePoints.add(_lt);
		this.insidePoints.add(_rb);		
	}


	public boolean add(Point pt) {
		this.insidePoints.add(pt);
		return true;
	}
	

	
	public double volume() {
		return Math.abs((this.getXMax()-this.getXMin())*(this.getYMax()-this.getYMin()));
	}

	public static double calculateVolume(MBR first, MBR sec) {
		PointSet portential = new PointSet();
		portential.addAll(sec.insidePoints);
		portential.addAll(first.insidePoints);
		ArrayList<Double> ptList = portential.range();
		Double XMin = ptList.get(0);
		Double XMax = ptList.get(1);
		Double YMin = ptList.get(2);
		Double YMax = ptList.get(3);
		Double TMin = ptList.get(4);
		Double TMax = ptList.get(5);
		return Math.abs((XMax-XMin)*(YMax-YMin)*(TMax-TMin));
	}
	
	public boolean merge(MBR m) {
		this.insidePoints.addAll(m.insidePoints);
		
		
		
		return true;
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
		return this.traID+ " _ " + this.seq+" " + this.getXMin() + " "+this.getXMax() + this.insidePoints.first().getTime()+" " + this.getYMin() +" " + this.getYMax() + this.insidePoints.last().getTime()+" "+ " total:"+ this.insidePoints.size()+ " \n";
	}

	public PointSet getInsidePoints() {
		return insidePoints;
	}

	public void setInsidePoints(PointSet insidePoints) {
		this.insidePoints = insidePoints;
		Iterator<Double> ite =	this.insidePoints.range().iterator();

	}

	public String getSeq() {
		return seq;
	}
	
	public LinearRing shape() {
		Coordinate firstArray[] = new Coordinate[5];
		
		firstArray[0] = new Coordinate(this.getXMin(), this.getYMin(), this.getTMin());
        firstArray[1] = new Coordinate(this.getXMax(), this.getYMin(), this.getTMin());
        firstArray[2] = new Coordinate(this.getXMax(), this.getYMax(), this.getTMin());
        firstArray[3] = new Coordinate(this.getXMin(), this.getYMax(), this.getTMin());
        firstArray[4] = new Coordinate(this.getXMin(), this.getYMin(), this.getTMax());

        SerializedEL traLayer = new Neo4JCon().getLayer();
        LinearRing shell = traLayer.getGeometryFactory().createLinearRing(firstArray);
        return shell;
	}






	public Double getXMin() {
		return this.insidePoints.range().get(0);
	}



	public Double getXMax() {
		return this.insidePoints.range().get(1);
	}

	public Double getYMin() {
		return this.insidePoints.range().get(2);
	}


	public Double getYMax() {
		return this.insidePoints.range().get(3);
	}

	public Double getTMin() {
		return this.insidePoints.range().get(4);
	}
	
	public Double getTMax() {
		return this.insidePoints.range().get(5);
	}


}
