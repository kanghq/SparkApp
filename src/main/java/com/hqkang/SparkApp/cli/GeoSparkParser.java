package com.hqkang.SparkApp.cli;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

public class GeoSparkParser extends SubmitParser {

	private int z = 0;
	Option stage = OptionBuilder.withArgName("Int").hasArg().withDescription("Debug stage").create("z");


	public GeoSparkParser(String[] args) {
		
		super(args);
		// TODO Auto-generated constructor stub
		super.options.addOption(stage);
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
