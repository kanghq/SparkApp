package com.hqkang.SparkApp.core;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Point implements Serializable{
	private double latitude;
	private double longitude;
	private Date time;
	
	Point(Date _time, double _latitude, double _longitude) {
		latitude = _latitude;
		longitude = _longitude;
		time = _time;
		
		
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
	
	public double getX() {
		return latitude;
	}
	
	public double getY() {
		return longitude;
	}

}
