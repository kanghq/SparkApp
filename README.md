# Trajectory Segmentation and Similarity Estimation Using Spark


This is a demo application for trajectory segmentation and similarity estimation. Its's based on Spark and Neo4J Graph Database. 

## Highlights

 1. Using Spark for parallelism.
 2. Using JTS topology Library to calculate two trajectory similarity.
 3. Support Neo4j Spatial to persist trajectory segmentation.


## Building

The simplest way to build Neo4j Spatial is by using maven. Just clone the git repository and run

    mvn install

This will download all dependencies, compiled the library, run the tests and install the artifact in your local repository. The application be created in the  `target`  directory, and can be copied to your local server or upload to AWS for execution.


## deployment into AWS

we recommend using EMR as Spark Environment.


Paper:




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
    
