#!/bin/bash

LOG=/dev/null
#rm $LOG

DATADIR=/home/x362liu/datasets
DIR=/home/x362liu

	#echo "================Load ${i}=========">>$LOG
    rm -rf db
    #>$DATADIR/essex/one.csv
    #size=$( echo "$i*2730"|bc) 
    #head -n $size $DATADIR/homeids.essex>$DATADIR/homeids.csv
    #for hid in $( cat /home/x362liu/datasets/homeids.csv )
    #do
     # cat  $DATADIR/essex/${hid}.csv>>$DATADIR/essex/one.csv
    #done
    { time $DIR/q/l64/q main_loadpart.q; } 2>>$LOG


