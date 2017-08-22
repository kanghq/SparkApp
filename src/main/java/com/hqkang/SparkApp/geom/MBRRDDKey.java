package com.hqkang.SparkApp.geom;

import java.text.SimpleDateFormat;
import java.util.Date;

import scala.Tuple2;

public class MBRRDDKey extends Tuple2<String, Integer>{


	private int startTime;
	private int endTime;
	public MBRRDDKey(String _1, int _2) {
		super(_1, _2);
		// TODO Auto-generated constructor stub
	}
	
	
	@Override
	public boolean equals(Object o) {
		if(!(o instanceof MBRRDDKey))
			return false;
		MBRRDDKey key = (MBRRDDKey) o;
		return this._1.equals(key._1)
				&& this._2.equals(key._2);
	}
	
	@Override
	public int hashCode() {
		return this._1.hashCode() +_2;
		
	}


	public int getStartTime() {
		return startTime;
	}


	public void setStartTime(double milSec) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		
		//this.startTime = Integer.parseInt(sdf.format(new Date((long)milSec)));
		this.startTime= (int) (milSec);
	}


	public int getEndTime() {
		return endTime;
	}


	public void setEndTime(double milSec) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");

		//this.endTime = Integer.parseInt(sdf.format(new Date((long)milSec)));
		 this.endTime = (int) (milSec);
	}
	



}
