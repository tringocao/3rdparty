#!/bin/bash
hdfs namenode -format
start-all.sh

hdfs dfs -mkdir /data
hdfs dfs -mkdir /data/input
hdfs dfs -mkdir /data/src
hdfs dfs -mkdir /data/input2
