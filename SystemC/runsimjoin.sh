#!/bin/bash

CURDIR=$( pwd )
LOGDIR=$CURDIR/logs

DATASET=syn1y
NUM_PER_GB=3200 #320 (syn 10year) 3200 (syn 1year) 2730 (Essex 1y)

DATAROOT=/home/x362liu/datasets
DATADIR=${DATAROOT}/${DATASET}
DATE="$(date +'%Y-%m-%d')"




prepareHomeidsimToMany(){
  rm -f $DATAROOT/homeids*.csv
  size=$( echo "$1*${NUM_PER_GB}"|bc ) # 1GB=320 csv files.
  psize=$(( ${size}/$2 ))
  for (( k=1; k<=$2; ++k ))
  do
     st=$(( ($k -1) * $psize + 1 ))  
     ed=$(( $k * $psize ))
     sed -n "${st}, ${ed}p" $DATAROOT/homeids.${DATASET} >$DATAROOT/homeidsim${k}.csv
  done
  head -n $size $DATAROOT/homeids.$DATASET >$DATAROOT/homeids.csv
}



meet(){
	 while true
	 do 
		 sleep 6 
		 cnt=$( grep real $2.*|wc -l ); 
		 if [ $cnt -eq $1 ]
		 then 
			break
		 fi
	 done
}

runModel(){
    TMP=$LOGDIR/$1_TMP
	rm -f ${TMP}.*
    for (( m=1; m<=$2; ++m ))
    do
		{ time  /home/x362liu/q/l64/q ${MODEL}.q -partno $m; } 2>>${TMP}.$m &
	done

    meet ${2} ${TMP}

	for (( n=1; n<=$2; ++n ))
    do
		echo "------ $3 GB ------" >>$LOGDIR/$1_${DATE}.$n
		cat ${TMP}.$n >> $LOGDIR/$1_${DATE}.$n
	done
    rm -f ${TMP}.*
}


NUM_OF_THREADS=8
MODELS=(similarity_join)
rm -f $LOGDIR/*_${DATE}*

for DATA_SIZE in 1 2 3 4 
do
	prepareHomeidsimToMany ${DATA_SIZE} ${NUM_OF_THREADS}
	for (( i=0; i<${#MODELS[@]}; ++i ))
    do
	  MODEL=${MODELS[${i}]}
	  runModel ${MODEL} ${NUM_OF_THREADS} ${DATA_SIZE}
	done
	clean
done
touch $CURDIR/DONE

