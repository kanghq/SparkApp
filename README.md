#Trajectory Segmentation and Similarity Estimation Using Spark


This is a demo application for trajectory segmentation and similarity estimation. Its's based on Spark and Neo4J Graph Database. 

## Algorithms
- Greedy-Split to segment trajectories into Minimum Bounding Rectangles(MBRs).
- Quad-tree repartitioing strategy to distribute MBRs.
- In-memory or external Nosql R-tree indexing.
- Multiple query metrics support.

## Highlights
 - Using Spark for parallelism.
 - Using JTS topology Library to calculate two trajectory similarity.
 - Support Neo4j Spatial to persist trajectory segmentations.
 
 More info:

- [Master Thesis](https://github.com/kanghq/SparkApp/blob/master/cuthesis.pdf)
- [Slides](https://github.com/kanghq/SparkApp/blob/master/pres.pdf)
- [Under Review Journal Paper](https://github.com/kanghq/SparkApp/blob/master/IEEE_Tran_jrnl.pdf)


## Building

The simplest way to build Neo4j Spatial is by using maven. Just clone the git repository and run

    mvn install

This will download all dependencies, compiled the library, run the tests and install the artifact in your local repository. The application be created in the  `target`  directory, and can be copied to your local server or upload to AWS for execution.


## deployment into AWS

we recommend using EMR as Spark Environment.

Software Version:emr-5.5.1

P.S. Please make sure you have enough disk space. 300GB or more for each node

Submit job:
`spark-submit --deploy-mode cluster \
    --class com.hqkang.SparkApp.core.Import s3://path_of_application/application.jar \  
    -i s3://path_to_geolife_trajectories/ -o s3://path_to_output_dir/ -s 20 -p 5000 -z 10`
    
    
    
    
    -i input path
    -o output path
    -z MBR margin
    -p Data load initial partition number
    -s Trajectory Segmentation number
    


