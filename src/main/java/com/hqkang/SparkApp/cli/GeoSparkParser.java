package com.hqkang.SparkApp.cli;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

public class GeoSparkParser extends SubmitParser {
	
	/*
	 * z = debug option 
	 */

	private int z = 0;
	Option margin = OptionBuilder.withArgName("Int").hasArg().withDescription("Margin of MBR").create("z");


	public GeoSparkParser(String[] args) {
		
		super(args);
		// TODO Auto-generated constructor stub
		super.options.addOption(margin);
    	options.addOption("y", false, "Save All");
    	
    	options.addOption("j", false, "Stat Switch");


	}
	
	
	public int getMargin() {
		return Integer.parseInt(cmd.getOptionValue("z"));
	}
	
	public boolean getSaveAll() {
		return cmd.hasOption("y");
	}
	
	public boolean getStat() {
		return cmd.hasOption("j");
	}

}
