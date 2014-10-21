#!/bin/bash

CURDIR=$( pwd )
LOGDIR=$CURDIR/logs

DATASET=essex
NUM_PER_GB=1300 #320 2730
DATAROOT=/home/x362liu/datasets
DATADIR=${DATAROOT}/${DATASET}
DATE="$(date +'%Y-%m-%d')"



homeids(){
	size=$(( $1*${NUM_PER_GB} ))  
    head -n $size $DATAROOT/homeids.$DATASET >$DATAROOT/homeids.csv
}

mergeToOne(){
	rm -rf $DATADIR/data.csv>/dev/null 2>&1
	for hid in $( cat $DATAROOT/homeids.csv )
	do
		cat $DATADIR/${hid}.csv >> $DATADIR/data.csv
	done
}

loaddata(){
    homeids $1
	rm -rf $CURDIR/db
	mkdir -p $CURDIR/db
    /home/x362liu/q/l64/q main_loadpart.q
}


#MODELS=(threelines par hist similarity)
MODELS=( threelines par hist )
NUM_OF_THREADS=8


rm -rf $LOGDIR && mkdir -p $LOGDIR

for DATA_SIZE in 20 40 60 80 100
do
	loaddata ${DATA_SIZE}
	for MODEL in ${MODELS[*]}
    do
	  LOG=$LOGDIR/${MODEL}_${DATA_SIZE}_${NUM_OF_THREADS}threads_${DATE}.log
	  echo "----------${DATA_SIZE} GB-------------">>$LOG
	  { time  /home/x362liu/q/l64/q ${MODEL}.q -s ${NUM_OF_THREADS}; } 2>> $LOG
	done
done

touch DONE
