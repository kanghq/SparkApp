package com.hqkang.SparkApp.core;

import org.apache.spark.serializer.KryoRegistrator;
import org.neo4j.gis.spatial.EditableLayer;

import com.esotericsoftware.kryo.Kryo;
import com.hqkang.SparkApp.geom.MBR;
import com.hqkang.SparkApp.geom.MBRList;
import com.hqkang.SparkApp.geom.Point;
import com.hqkang.SparkApp.geom.PointSet;

public class MyRegistrator implements KryoRegistrator{

	@Override
	public void registerClasses(Kryo k) {
		// TODO Auto-generated method stub
		k.register(EditableLayer.class);
		k.register(MBR.class);
		k.register(MBRList.class);
		k.register(Point.class);
		k.register(PointSet.class);



	}

}
