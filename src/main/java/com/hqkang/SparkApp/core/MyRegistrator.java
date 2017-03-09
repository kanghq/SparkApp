package com.hqkang.SparkApp.core;

import org.apache.spark.serializer.KryoRegistrator;
import org.neo4j.gis.spatial.EditableLayer;

import com.esotericsoftware.kryo.Kryo;

public class MyRegistrator implements KryoRegistrator{

	@Override
	public void registerClasses(Kryo k) {
		// TODO Auto-generated method stub
		k.register(EditableLayer.class);
		k.register(MBR.class);
		k.register(MBRList.class);
		k.register(Point.class);



	}

}
