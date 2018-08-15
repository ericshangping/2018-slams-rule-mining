#!/bin/bash
#hadoop programs dont run if the output directory specified already exists so I delete them first
#hdfs dfs -rm -r /user/dxxsha001/IBMDataset/inputCondFPTrees
hdfs dfs -rm -r /user/dxxsha001/IBMDataset/output*
hadoop jar AssociationRules.jar FPGrowthFrequentItemsets.FPGrowthMain /user/dxxsha001/IBMDataset/input /user/dxxsha001/IBMDataset/output 30 10
#hdfs dfs -cat /user/dxxsha001/IBMDataset/output1/*
#copies from hdfs to local dir
#hadoop fs -copyToLocal /user/dxxsha001/IBMDataset/output1/part-r-00000 /local/directory
#puts files onto hdfs
#hdfs dfs -put /user/desktop/localfile /user/dxxsha001/hdfs/directory
# hdfs dfs -copyToLocal /user/dxxsha001/IBMDataset/outputOrderedItemsets/* .
