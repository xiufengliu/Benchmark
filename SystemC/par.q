// ############## Define the analytics functions ##########
round:{floor x+0.5};
range:{(min x;max x)};
quantile:{[x;p]  xs:asc distinct x; n:count xs; 0.5*sum xs (floor n*p;1+floor n*p)};
hist:{[x;nbins] count each group (abs (-) . range x % nbins) xbar (asc x) };
linregr:{[y;X]if[any[null y:"f"$y]|any{any null x}'[X:"f"$X];'`nulls]; if[$[0=m:count X;1;m>n:count X:flip X];'`length]; Z:inv[flip[X]mmu X];ZZ:X mmu Z mmu flip[X]; e:y- yhat:X mmu beta:Z mmu flip[X] mmu y;``S`beta`e`n`m`df`ZZ`Z`yhat!(::;Z*mmu[e;e]%n-m;beta;e;n;m;n-m;ZZ;Z;yhat)};

/fit an autoregressive time series model to the data by OLS, returns the parameter vector
arOLS:{[x;p;i] X:{[x;p;y] p _ y xprev x}[x;p;]@/:1+til p;Y:enlist p _ x;if[i;X,:(count Y)#1f];Y lsq X};

/generate log normal distribution
logNorm:{[m;v;x] mu:log[(m2)%sqrt[v+m2:m*m]]; sigma:sqrt[log 1+v%m2];:exp(mu+sigma*(sqrt[-2*log x?1f])*cos(2*PI*x?1f))};

/algorithm poisson random number (Knuth)
knuth:{[x;s]system"S ",string s;i:count x;k:i#0;p:i#1f;L:exp neg[x];while[any g:p>L;p:@[p;h:where g;*;(sum g)?1f];k:@[k;h;+;1]];:k-1};



par:{[homeid];
    // data:select readtime, reading from essex where int=homeid; //used in memory
    // data:flip `readtime`reading!(raze data[`readtime]; raze data[`reading]); //used in memory
    // d: select reading by readtime from data; //used in memory

    d:select reading by readtime from essex where int=homeid; //used for select database
    tmpresults:();
    seasons:24;
    if[seasons=count d;
        s:0;
        do[seasons;
          phi:raze arOLS[d[s][`reading];3;0];
          tmpresults,:enlist (homeid, s, phi[0], phi[1], phi[2]);
          s:s+1;
          ];  
      :tmpresults;
    ]
 };


// ########### Main #################
results:([]homeid:`int$(); season:`int$(); beta1:`float$();beta2:`float$();beta3:`float$());

homeids:("II";",") 0: `:/home/x362liu/datasets/homeids.csv;
homeids:homeids[0];


// Test when all the data is kept in main memory
// essex:flip `int`readdate`readtime`reading`temperature!("IDIFF"; ",")0:`:/home/x362liu/datasets/syn10y/data.csv;
// essex:select readtime, reading, temperature by int from essex;

\l /home/x362liu/kdb/db

st:.z.T;
rs:par peach homeids;
i:0;
do[count rs;
    j:0;
    do[count rs[i];
        `results insert (rs[i][j][0];rs[i][j][1];rs[i][j][2];rs[i][j][3];rs[i][j][4]);
        j:j+1;
     ];
    i:i+1;
  ];
save `:/home/x362liu/kdb/results.csv;
ed:.z.T;

show "Time=";
show ed-st;

\\

