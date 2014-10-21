loaddata:{[homeid]
   fname:`$"" sv(":/home/x362liu/datasets/essex/";string homeid;".csv");
   data:flip `homeid`readdate`readtime`reading`temperature!("IDIFF"; ",")0:fname;
   .Q.dpft[`:/home/x362liu/kdb/db/;homeid;`homeid;`data];
   delete data from `.;
   };

homeids:("II";",") 0: `:/home/x362liu/datasets/homeids.csv;
homeids:homeids[0];

st:.z.T;
i:0;
do[count homeids;
    homeid:homeids[i];
    fname:`$"" sv(":/home/x362liu/datasets/essex/";string homeid;".csv");
    essex:flip `homeid`readdate`readtime`reading`temperature!("IDIFF"; ",")0:fname;
    .Q.dpft[`:/home/x362liu/kdb/db/;homeid;`homeid;`essex];
    delete essex from `.;
    i:i+1;
  ]
ed:.z.T;
show (ed-st); 
\\

