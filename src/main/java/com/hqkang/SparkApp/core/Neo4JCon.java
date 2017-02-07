package com.hqkang.SparkApp.core;

import java.io.File;
import java.io.Serializable;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

import org.neo4j.gis.spatial.SpatialDatabaseService;
import org.neo4j.gis.spatial.encoders.SimpleGraphEncoder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.IndexManager;

public class Neo4JCon implements Serializable{
	static ResourceBundle rb = ResourceBundle.getBundle("Config");

	
	static String dbPath = "/Users/kanghuaqiang/Downloads/neo4j-community-3.1.0/data/databases/graph.db/";


	
	private static final File storeDir = new File(rb.getString("dbPath"));

	private static final GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabase(storeDir);

	private static final SpatialDatabaseService spatialService = new SpatialDatabaseService(
			database);
	
	
	private static final SerializedEL traLayer = (SerializedEL) spatialService.getOrCreateLayer("Tra", SimpleGraphEncoder.class, SerializedEL.class);

	public Neo4JCon() {
		String[] properties = {"Seq", "TraID", "StartTime", "EndTime"};
		traLayer.setExtraPropertyNames(properties);
		
	}
	
	public SerializedEL getLayer() {
		return traLayer;
	}
	
	public GraphDatabaseService getDb() {
		return database;
	}
}
