package com.hqkang.SparkApp.core;

import java.io.Serializable;

public class Test implements Serializable{
	private Test s;
	public Test(int t) {
		s = new Test();
	}
	public Test() {
	}
	public Test getS() {
		return s;
	}
	public void setS(Test s) {
		this.s = s;
	}
	

}
