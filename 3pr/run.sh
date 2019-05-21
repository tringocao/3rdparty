#!/bin/bash
path=$PWD 

#$1 = times 
	# Remove exist file in hdfs
	hdfs dfs -rm /data/input/*
	hdfs dfs -rm /data/input2/*
	hdfs dfs -rm /data/src/*

	# Put from local to hdfs
	hdfs dfs -put $path/input/*  /data/input

	hdfs dfs -put $path/src/query.txt  /data/src
# Compile java file
cd $path/src
hadoop com.sun.tools.javac.Main kHSimHelper.java SpiralClusterBuilder.java ClusterItem.java
# Make jar file and run
jar cf SCB.jar SpiralClusterBuilder*.class kHSimHelper.class ClusterItem.class
hadoop jar SCB.jar SpiralClusterBuilder /data/input /output0$1
# Show output0
#hdfs dfs -cat /output0$3/part*
# Copy from HSDF to local
rm $path/res/result/*
hdfs dfs -get /output0$1/part* $path/res/result/result
# Compile and run 3rd-party process
javac MiddleProcess.java
java MiddleProcess $path
# Put MidProcessRes to HDFS
hdfs dfs -put $path/res/midProcessRes  /data/input2

#each distributed periods
cd $path/distributed
javac preMR23.java
java preMR23 $path

# Put sentHDFS2,3 to HDFS
hdfs dfs -put $path/res/sentHDFS2  /data/input2
hdfs dfs -put $path/res/sentHDFS3  /data/input2
# Compile java file
hadoop com.sun.tools.javac.Main kHSimHelper.java MapReduceJob2.java MapReduceJob3.java ClusterItem.java
# Make jar file and run
jar cf MR23.jar MapReduceJob2*.class MapReduceJob3*.class kHSimHelper.class ClusterItem.class
hadoop jar MR23.jar MapReduceJob2 /data/input2/sentHDFS2 /output_MR2_$1 /data/input2/sentHDFS3 /output_MR3_$1 $path
