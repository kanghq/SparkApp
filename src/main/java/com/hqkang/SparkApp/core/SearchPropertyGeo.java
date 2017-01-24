package com.hqkang.SparkApp.core;

import org.neo4j.gis.spatial.rtree.Envelope;
import org.neo4j.gis.spatial.rtree.filter.SearchFilter;
import org.neo4j.graphdb.Node;

public class SearchPropertyGeo implements SearchFilter {
	
	protected String propertyName;
	protected Object propertyVaule;
	protected String operation;
	
	public SearchPropertyGeo(String propertyName, Object value, String ope) {
		this.propertyName = propertyName;
		this.propertyVaule = value;
		this.operation = ope;
	}

	@Override
	public boolean geometryMatches(Node otherNode) {
		// TODO Auto-generated method stub
		Object leftObject = otherNode.getProperty(propertyName);
		switch (operation) {
        case "EQUAL":
            if (null == leftObject)
                return false;
            return leftObject.equals(propertyVaule);
        case "NOT_EQUAL":
            if (null == leftObject)
                return propertyVaule != null;
            return !leftObject.equals(propertyVaule);
        case "GREATER_THAN":
            if (null == leftObject || propertyVaule == null)
                return false;
            return ((Comparable) leftObject).compareTo(propertyVaule) == 1;
        case "LESS_THAN":
            if (null == leftObject || propertyVaule == null)
                return false;
            return ((Comparable) leftObject).compareTo(propertyVaule) == -1;
        case "GREATER_THAN_EQUAL":
            if (null == leftObject || propertyVaule == null)
                return false;
            return ((Comparable) leftObject).compareTo(propertyVaule) >= 0;
        case "LESS_THAN_EQUAL":
            if (null == leftObject || propertyVaule == null)
                return false;
            return ((Comparable) leftObject).compareTo(propertyVaule) <= 0;
        default:
            throw new IllegalArgumentException("Invalid state as no valid filter was provided");
		}
		
	}	

	@Override
	public boolean needsToVisit(Envelope arg0) {
		// TODO Auto-generated method stub
		return true;
	}

}
