CS8803 Final project source code

1. BDH.R
This is a R script which generated all the statistics and figures in the paper.

The input data file events.csv is downloaded through pgAdimIII from 
sunlab.cncuaxfkq7hp.us-east-1.rds.amazonaws.com.

The required packages are:
caret, reshape, ggbiplot, randomForest, kmeans, cluster



2. BDH.zip
This is a spark deployment of the above R script.
It has realized data preparation, feature extraction and DRE prediction.

The required libs are:
spark-assembly-1.2.1-hadoop2.5.1-mapr-1501.jar
hadoop-common-2.2.0.jar

The JRE version is JavaSE-1.6

I run it on a mapr Hadoop cluster.
