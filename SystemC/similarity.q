// ############## Define the analytics functions ##########
homeids:("II";",") 0: `:/home/x362liu/datasets/homeids.csv;
homeids:homeids[0];

homeidsim:("II";",") 0: `:/home/x362liu/datasets/homeidsim.csv;
homeidsim:homeidsim[0];


// Test when all the data is kept in main memory
// essex:flip `int`readdate`readtime`reading`temperature!("IDIFF"; ",")0:`:/home/x362liu/datasets/syn10y/data.csv;
// essex:select reading by int from essex;

\l /home/x362liu/kdb/db

similarity:{[stIndex];
    rs:();
    size:count homeids;
    // data:select reading from essex where int=homeids[stIndex]; //In-mem
	// X:raze raze data[`reading]; //In-mem

    X:select reading from essex where int=homeidsim[stIndex]; //In-db
    X:X[`reading]; // In-db
    i:0;
    do[size;
        // data:select reading from essex where int=homeids[i]; //In-mem
        // Y:raze raze data[`reading]; //In-mem
        Y:select reading from essex where int=homeids[i]; //In-db
        Y:Y[`reading]; //In-db
        s:min(count X; count Y);
        dotXY:sum (s#X)*(s#Y);
        normXY:(sqrt sum s#X*X)*(sqrt sum s#Y*Y);
        if[normXY>0.0;
            rs,:enlist (homeidsim[stIndex], homeids[i], dotXY%normXY);
           ];
        i:i+1
      ];
    :rs;
 };



// ########### Main #################
results:([]thishomeid:`int$(); thathomeid:`int$(); similarity:`float$());


st:.z.T;
indexes:til count homeidsim;
rs:similarity peach indexes;
i:0;
do[count rs;
    j:0;
    do[count rs[i];
        `results insert (rs[i][j][0];rs[i][j][1];rs[i][j][2]);
        j:j+1
        ];
    i:i+1;
    ];

results:ungroup `thishomeid xdesc ?[`thishomeid`similarity xdesc results;();b!b:enlist `thishomeid;] a!(10#),/:  a: cols[ results] except `thishomeid;
save `:/home/x362liu/kdb/results.csv;
ed:.z.T;

show "Time=";
show ed-st;

\\

