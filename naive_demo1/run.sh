#!/bin/bash
START_TIME=$SECONDS
path=$PWD
#$1 = times 

hdfs dfs -mkdir /data
hdfs dfs -mkdir /data/input_naive
hdfs dfs -mkdir /data/src_naive


# Put from local to hdfs
hdfs dfs -put $path/input/*  /data/input_naive

# Run
cd $path/src
hadoop jar NMR.jar NaiveMapReduce /data/input_naive/ /output_naive0$1

# Copy from HSDF to local
rm $path/res/*
hdfs dfs -get /output_naive0$1/part* $path/res

ELAPSED_TIME=$(($SECONDS - $START_TIME))

echo "$(($ELAPSED_TIME/60)) min $(($ELAPSED_TIME%60)) sec"    

