package com.hqkang.SparkApp.core;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Point implements Serializable, Comparable{
	
	private double latitude;
	private double longitude;
	private Date time;
	private int seq;
	
	Point() {
	}
	
	Point(Date _time, double _latitude, double _longitude) {
		latitude = _latitude;
		longitude = _longitude;
		time = _time;
		
		
	}
	
	Point(double _lat, double _lon) {
		latitude = _lat;
		longitude = _lon;
	}
	
	Point(String _date, String _time, String _latitude, String _longitude) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			time = sdf.parse(_date + " " + _time);
			latitude = Double.parseDouble(_latitude);
			longitude = Double.parseDouble(_longitude);

		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public double X() {
		return latitude;
	}
	
	public double Y() {
		return longitude;
	}
	
	public Date T() {
		return time;
	}
	
	public void setSeq(int t){
		this.seq = t;
	}

	@Override
	public int compareTo(Object o) {
		Point p = (Point)o;
		if(this.time ==null || p.time ==null) {
			System.out.println(this.time);
			System.out.println(p.time);
		}
		if(this.time.after(p.time))  {
			return 1;
		} else return -1;
		
	}
	
	public String toString() {
		return this.latitude+":"+this.longitude+"@"+this.time;
	}

	/*Javabean generated*/
	
	public double getLatitude() {
		return latitude;
	}

	public void setLatitude(double latitude) {
		this.latitude = latitude;
	}

	public double getLongitude() {
		return longitude;
	}

	public void setLongitude(double longitude) {
		this.longitude = longitude;
	}

	public Date getTime() {
		return time;
	}

	public void setTime(Date time) {
		this.time = time;
	}

	public int getSeq() {
		return seq;
	}
	

}