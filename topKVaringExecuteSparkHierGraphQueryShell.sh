#!/bin/bash


echo "Start Execution..."

#k=2
#--master spark://192.168.0.51:8070  \
for k in {1,2,5,10,15,20,25,30}
    do
    base=10
    for i in {0..9}
	do

	suffixIndex=$(($k*$base+$i))

	$SPARK_HOME/bin/spark-submit \
	  --class "main.scala.hierarchialQuery.QueryMain" \
	  --master local[12]    \
        /home/fubao/workDir/ResearchProjects/GraphQuerySearchRelatedPractice/SparkDistributedPractice/hierarchicalQueryDistributedSpark/target/scala-2.11/hierarchialquery-project_2.11-1.0.jar $k $suffixIndex

	sleep 3                                               #wait execution finishes above
    done

done

echo "End"

