#!/bin/bash
path=$PWD
#$1 = times 

# Compile java file
cd $path/src
hadoop com.sun.tools.javac.Main NaiveMapReduce.java kHSimHelper.java ClusterItem.java
jar cf NMR.jar NaiveMapReduce*.class kHSimHelper.class ClusterItem.class


