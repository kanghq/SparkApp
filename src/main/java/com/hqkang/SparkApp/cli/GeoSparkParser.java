package com.hqkang.SparkApp.cli;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

public class GeoSparkParser extends SubmitParser {

	private int z = 0;
	Option nsrv = OptionBuilder.withArgName("Int").hasArg().withDescription(" Neo4j Srv Number ").create("r");


	public GeoSparkParser(String[] args) {
		
		super(args);
		// TODO Auto-generated constructor stub
    	options.addOption("y", false, "Save All");
    	super.options.addOption(nsrv);

	}
	
	
	
	public int getNeo4jSrv() {
		return Integer.parseInt(cmd.getOptionValue("r"));
	}
	
	public boolean getSaveAll() {
		return cmd.hasOption("y");
	}

}
