package com.hqkang.SparkApp.geom;

import java.io.Serializable;

public class MBRKey implements Serializable{
	MBR k1 = null;
	MBR k2 = null;
	
	MBRKey(MBR k1, MBR k2){
		this.k1 = k1;
		this.k2 = k2;
	}
	
	@Override
	public boolean equals(Object o) {
		if(!(o instanceof MBRKey))
			return false;
		MBRKey key = (MBRKey) o;
		return this.k1.equals(key.k1)
				&& this.k2.equals(key.k2);
	}
	
	@Override
	public int hashCode() {
		return k1.hashCode()+k2.hashCode();
		
	}

}
