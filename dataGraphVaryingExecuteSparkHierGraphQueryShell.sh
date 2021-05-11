#!/bin/bash


echo "Start Execution..."

k=5
#--master spark://192.168.0.51:8070  
#gs: data graph size
for gs in {1,2,5,8,10}
    do
    base=10
    for i in {0..9}
	do

	suffixIndex=$(($gs*$base+$i))

	$SPARK_HOME/bin/spark-submit \
	  --class "main.scala.hierarchialQuery.QueryMain" \
	  --master local[12]    \
        /home/fubao/workDir/ResearchProjects/GraphQuerySearchRelatedPractice/SparkDistributedPractice/hierarchicalQueryDistributedSpark/target/scala-2.11/hierarchialquery-project_2.11-1.0.jar $k $suffixIndex $gs

	sleep 3                                               #wait execution finishes above
    done

done

echo "End"

