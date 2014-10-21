#!/bin/bash

#algorithms=( ThreelMain PARMain HistogramMain CosineMain )
algorithms=( ThreelMain PARMain HistogramMain )
node_num=16

export HADOOP_HOME=/work/afancy/hadoop
export HIVE_HOME=/work/afancy/hive
export HADOOP_YARN_HOME=/work/afancy/hadoop

restartYarn(){
	/work/afancy/hadoop/sbin/stop-yarn.sh  >/dev/null 2>&1
	/work/afancy/hadoop/sbin/start-yarn.sh  >/dev/null 2>&1
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
	/work/afancy/hive/bin/hive -hiveconf mapreduce.job.reduces=40 -f hql/preparedataforhive.sql
	/work/afancy/hadoop/sbin/stop-yarn.sh  >/dev/null 2>&1
	sleep 20
}

prepareFirstFormatData_tmp(){
	if [[ $1 -eq 2600 ]]; then
	 hadoop fs -mv /user/afancy/warehouse/tbl_firstformat/*.csv /user/afancy/warehouse/m26files/  >/dev/null 2>&1
	fi;
	if [[ $1 -eq 26000 ]]; then
		hadoop fs -mv /user/afancy/warehouse/tbl_firstformat/*.csv /user/afancy/warehouse/m2600files/  >/dev/null 2>&1
	fi;
	hadoop fs -mv /user/afancy/warehouse/m${1}files/*.csv /user/afancy/warehouse/tbl_firstformat/  >/dev/null 2>&1
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
	#prepareFirstFormatData_tmp $size
	
	for alg in ${algorithms[*]}
	do
		LOG=logs/${alg}/hive_${alg}_${size}GB_${node_num}nodes_1stformat.log
		rm -f $LOG && echo $LOG
		restartYarn
		/usr/local/hive/bin/hive -hiveconf mapreduce.job.reduces=16 -f hql/${alg}_1format.sql  2>&1 | tee $LOG
		
		
		#LOG=logs/${alg}/hive_${alg}_${size}files_${node_num}nodes_3rdformat.log
		#rm -f $LOG && echo $LOG
		#restartYarn
		#/work/afancy/hive/bin/hive -hiveconf mapreduce.job.reduces=8 -f hql/${alg}_3format.sql  2>&1 | tee $LOG
		
		#clean
		#/work/afancy/hadoop/bin/hadoop jar smas-benchmark-1.0-SNAPSHOT.jar ca.uwaterloo.iss4e.hadoop.pointperrow.${alg} hdfs://gho1:9000/user/afancy/warehouse/tbl_firstformat hdfs://gho1:9000/user/afancy/warehouse/output  2>&1 | tee $LOG
	
	
		#LOG=logs/${alg}/hive_${alg}_${size}GB_${node_num}nodes_2ndformat.log
		#rm -f $LOG && echo $LOG
		#restartYarn
		#/work/afancy/hive/bin/hive -hiveconf mapreduce.job.reduces=20 -f hql/${alg}.sql  2>&1 | tee $LOG
	done
done

touch HIVEDONE
