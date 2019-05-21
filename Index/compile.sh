#!/bin/bash
path=$PWD
hadoopvar=/home/tri/hadoop1/bin/hadoop

# Compile java file

$hadoopvar com.sun.tools.javac.Main Native1.java
jar cf NT1.jar Native1*.class

$hadoopvar com.sun.tools.javac.Main Native2.java
jar cf NT2.jar Native2*.class

javac Native3.java
