package com.hqkang.SparkApp.core;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.DataFrame;

public class ImportRawData {
	public void ReadAllFile(String filePath) {  
        File f = null;  
        f = new File(filePath);  
        File[] files = f.listFiles(); // 得到f文件夹下面的所有文件。  
        List<File> list = new ArrayList<File>();  
        for (File file : files) {  
            if(file.isDirectory()) {  
                //如何当前路劲是文件夹，则循环读取这个文件夹下的所有文件  
                ReadAllFile(file.getAbsolutePath());  
            } else {  
                list.add(file);  
            }  
        }  
       
    } 
	public static DataFrame rawDataFrame(String filderPath, final String delimiter) throws Exception {}

}
