#!/bin/bash

#export HADOOP_HOME=/work/afancy/hadoop
#export HIVE_HOME=/work/afancy/hive


executormem=3g
coresperexecutor=20
node_num=16

# ThreelMain PARMain HistogramMain CosineMain
algorithms=( ThreelMain )


restartSpark(){
	/work/afancy/spark/sbin/stop-all.sh>/dev/null 2>&1
	sleep 3
	/work/afancy/spark/sbin/start-all.sh>/dev/null 2>&1
	sleep 20
	echo "Spark was restarted"
}

prepareFirstFormatData(){
	hadoop fs -mv /user/afancy/warehouse/tbl_firstformat/*.csv /user/afancy/warehouse/data500gb/ >/dev/null 2>&1
	for (( j=$1; j<=$2; ++j ))
	do
		hadoop fs -mv /user/afancy/warehouse/data500gb/$j.csv /user/afancy/warehouse/tbl_firstformat >/dev/null 2>&1
	done
	echo "Finished moving files $1.csv -- $2.csv"
}

prepareSecondFormatData(){
	/work/afancy/hadoop/sbin/start-yarn.sh  >/dev/null 2>&1
	sleep 20
	/work/afancy/hive/bin/hive -hiveconf mapreduce.job.reduces=40 -f hql/preparedataforspark.sql
	/work/afancy/hadoop/sbin/stop-yarn.sh  >/dev/null 2>&1
	sleep 20
}


clean(){
	hadoop fs -rm -R /user/afancy/warehouse/output >/dev/null 2>&1
}

cd ~

for (( size=20; size<=100; size=size+20 ))
do
	ed=$(( $size/2 ))
	#ed=$size
	prepareFirstFormatData  1 $ed
	#prepareSecondFormatData
	
	for alg in ${algorithms[*]}
	do
		LOG=logs/${alg}/spark_${alg}_${size}files_${node_num}nodes_1stformat.log
		rm -f $LOG
		echo $LOG
		clean
		restartSpark
		/work/afancy/spark/bin/spark-submit --class ca.uwaterloo.iss4e.spark.pointperrow.${alg} --master spark://gho1:7077 --num-executors 16 --driver-memory 3g --executor-memory $executormem --executor-cores $coresperexecutor --jars /work/afancy/spark/lib/commons-math-2.2.jar smas-benchmark-1.0-SNAPSHOT.jar hdfs://gho-admin:9000/user/afancy/warehouse/tbl_firstformat hdfs://gho-admin:9000/user/afancy/warehouse/output 2>&1 | tee $LOG

		
		#LOG=logs/${alg}/spark_${alg}_${size}GB_${node_num}nodes_2ndformat.log
		#rm -f $LOG
		#echo $LOG
		#clean
		#restartSpark
		#/work/afancy/spark/bin/spark-submit --class ca.uwaterloo.iss4e.spark.meterperrow.${alg} --master spark://gho1:7077 --num-executors 20 --driver-memory 3g --executor-memory $executormem --executor-cores $coresperexecutor --jars /work/afancy/spark/lib/commons-math-2.2.jar smas-benchmark-1.0-SNAPSHOT.jar hdfs://gho1:9000/user/afancy/warehouse/tbl_secondformat hdfs://gho1:9000/user/afancy/warehouse/output 2>&1 | tee $LOG
	done
done

touch SPARKDONE
