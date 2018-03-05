# SparkApp

Sample EMR execution command
spark-submit --deploy-mode cluster \
        --conf "spark.executor.extraJavaOptions=-Djava.awt.headless=true -Dawt.toolkit=sun.awt.HToolkit" \
        --conf "spark.driver.extraJavaOptions=-Djava.awt.headless=true -Dawt.toolkit=sun.awt.HToolkit" \
        --executor-memory 15g --conf spark.yarn.executor.memoryOverhead=5g \
        --class com.hqkang.SparkApp.core.Import_rp s3://i-09fa3a04098d2f26b/test.jar \
        -i s3://i-09fa3a04098d2f26b/200M/*/*/* -o s3://i-09fa3a04098d2f26b-output/neo4j/ -s 200 -r 4 -p 160 -q 24
        
        
        
        
        
        -s trajectory segmentation parameter
        -r server node number
        -p R-tree partition number
        -q R-tree Layer time interval (hour)
