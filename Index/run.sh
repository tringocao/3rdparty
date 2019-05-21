#!/bin/bash
START_TIME=$SECONDS

path=$PWD
#$1 = times 

# Remove exist file in hdfs
hdfs dfs -rm /data/input/*
hdfs dfs -rm -r /data/input2

hdfs dfs -rm /data/src/*

	# Put from local to hdfs
	hdfs dfs -put $path/input/*  /data/input
	hdfs dfs -put $path/query.txt  /data/src

    	hadoop jar NT1.jar Native1 /data/input/ /data/input2


    	hadoop jar NT2.jar Native2 /data/src/query.txt  /output-MR2-0$1

hdfs dfs -get /output-MR2-0$1/part* $path/res/re.txt

java Native3 $path
ELAPSED_TIME=$(($SECONDS - $START_TIME))

echo "$(($ELAPSED_TIME/60)) min $(($ELAPSED_TIME%60)) sec"    




