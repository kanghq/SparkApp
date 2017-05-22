package com.hqkang.SparkApp.cli;
import java.util.logging.Logger;

import org.apache.commons.cli.*;

public class SubmitParser {
	
    protected String[] args = null;
    protected Options options = new Options();
    protected CommandLineParser parser = new BasicParser();
    protected CommandLine cmd = null;
    
    String iPath = "";
    String oPath = "";
    int k = 20;
    
	Option inputPath = OptionBuilder.withArgName("dir").hasArg().withDescription(" Trajectories File Path ").create("i");
	Option outputPath = OptionBuilder.withArgName("dir").hasArg().withDescription(" Result Output Path ").create("o");
	Option segNum = OptionBuilder.withArgName("Num").hasArg().withDescription(" Segmentation Number ").create("s");
	Option accessID = OptionBuilder.withArgName("String").hasArg().withDescription("S3 AccessKeyID").create("a");
	Option secretKey = OptionBuilder.withArgName("String").hasArg().withDescription("S3 Access Secret").create("e");
	Option part = OptionBuilder.withArgName("Int").hasArg().withDescription("Spark Partitions #").create("p");
	
    public SubmitParser(String[] args) {
    	options.addOption(inputPath);
    	options.addOption(outputPath);
    	options.addOption(segNum);
    	options.addOption("d", false, "Debug Mode");
    	options.addOption(accessID);
    	options.addOption(secretKey);
    	options.addOption(part);
    	this.args = args;
    	
    }
    
    public void parse() {
    	try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
    
    public String getIPath() {
    	return cmd.getOptionValue("i");
    }
    
    public String getOPath() {
    	return cmd.getOptionValue("o");
    }
    
    public int getSegNum() {
    	return Integer.parseInt(cmd.getOptionValue("s"));
    }
    
    public boolean getDebug() {
    	return cmd.hasOption("d");
    }
    
    public String getAccessID() {
    	return cmd.getOptionValue("a");
    }
    
    public String getSecretKey() {
    	return cmd.getOptionValue("e");
    }

	public int getPart() {
		// TODO Auto-generated method stub
		return Integer.parseInt(cmd.getOptionValue("p"));
	}

}
