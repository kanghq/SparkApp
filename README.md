# SparkApp



`spark-submit --deploy-mode cluster \
    --conf "spark.executor.extraJavaOptions=-Djava.awt.headless=true -Dawt.toolkit=sun.awt.HToolkit" \  
    --conf "spark.driver.extraJavaOptions=-Djava.awt.headless=true -Dawt.toolkit=sun.awt.HToolkit" \  
    --executor-memory 40g --conf spark.yarn.executor.memoryOverhead=10g --class com.hqkang.SparkApp.core.Import s3://i-09fa3a04098d2f26b/test.jar \  
    -i s3://i-09fa3a04098d2f26b/1G/*/*/* -o s3://i-09fa3a04098d2f26b-output/quadtree/ -s 20 -p 5000 -z 10`
    
    
    
    
    -i input path
    -o output path
    -z MBR margin
    -p Data load initial partition number
    -s Trajectory Segmentation number
    
