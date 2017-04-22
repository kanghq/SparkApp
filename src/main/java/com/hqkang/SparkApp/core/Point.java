package com.hqkang.SparkApp.core;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.GeometryFactory;

public class Point  implements Serializable, Comparable{
	
	private double latitude;
	private double longitude;
	private Date time;
	private int seq;
	private 
	
	Point() {}
	
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
		return longitude;
	}
	
	public double Y() {
		return latitude;
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
	
	public com.vividsolutions.jts.geom.Point toJTSPoint() {
		Coordinate coo = new Coordinate(this.latitude, this.longitude);
		GeometryFactory fact = new GeometryFactory();
		return fact.createPoint(coo);
	}
	
	public double distance(Point pt2) {
		Double el1 = 0.0;
		Double el2 = 0.0;
		Double lat1 = this.latitude;
		Double lat2 = pt2.latitude;
		Double lon1 = this.longitude;
		Double lon2 = pt2.longitude;

	    final int R = 6371; // Radius of the earth

	    Double latDistance = Math.toRadians(lat2 - lat1);
	    Double lonDistance = Math.toRadians(lon2 - lon1);
	    Double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
	            + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
	            * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
	    Double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
	    double distance = R * c * 1000; // convert to meters

	    double height = el1 - el2;

	    distance = Math.pow(distance, 2) + Math.pow(height, 2);

	    return Math.sqrt(distance);
	}

}
