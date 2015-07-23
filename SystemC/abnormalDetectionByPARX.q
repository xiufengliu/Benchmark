result:([]meterid:"i"$();readdate:"d"$();propability:"f"$());
t:([] meterid:(); hour:(); dist:([]reading:()));
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

print:{[message] 0N! message;};

XT1:{$[x>20;x-20;0]};

XT2:{$[x<16;16-x;0]};

XT3:{$[x<5;5-x;0]};

trainOneHour:{[readings;temperatures]
    p:3;
    size:(count readings)- p;
    if[size<20; :7#0];
    idx:til size; 
    y:readings[idx]; 
    X:(enlist size#1); 
    i:idx; 
    do[p; i:i + 1; X:X,enlist readings[i]]; 
    X:X,enlist XT1 each temperatures[idx]; 
    X:X,enlist XT2 each temperatures[idx]; 
    X:X,enlist XT3 each temperatures[idx]; 
    $[(count y)=count X[0]; :(linregr[y;X])[`beta]; :7#0]
    };

hourlyL2dist:{[row]  
	p:3; 
	readings:row[`reading];
	temperatures:row[`temperature]; 
	beta:row[`beta]; 
    if[sum[beta]=0; :`meterid`mean`theta!(row[`meterid];row[`hour];0)];
    size:(count readings)- p; 
	idx:til size;
	actual:readings[idx]; 
    predict:beta[0] + (beta[1]*readings[idx+1])+(beta[2]*readings[idx+2])+(beta[3]*readings[idx+3])+(beta[4]*XT1 each temperatures[idx])+(beta[5]*XT2 each temperatures[idx])+(beta[6]*XT3 each temperatures[idx]); 
    `t insert (row[`meterid];row[`hour];enlist (actual-predict)*(actual-predict)); 
  /	:`meterid`hour`dist!(row[`meterid];row[`hour];(actual-predict)*(actual-predict)) 
	};

normalDist:{[row] :`meterid`mean`theta!(row[`meterid]; avg[row[`dist]]; sqrt[var[row[`dist]]])};

train:{[tstable]
	tstable:`meterid`readtime xdesc tstable;
	.Q.gc[];

	parameters:select beta:trainOneHour[reading;temperature] by meterid, hour:readtime.hh from tstable;
    `:/home/x362liu/kdb/parameters set parameters;
    / `t hourlyL2dist/: () xkey parameters ij (select reading,temperature by meterid, hour:readtime.hh from tstable);
    hourlyL2dist each () xkey parameters ij (select reading,temperature by meterid, hour:readtime.hh from tstable);
    / l2dist: hourlyL2dist each () xkey parameters ij (select reading,temperature by meterid, hour:readtime.hh from tstable);

    
  /	l2dist:select dist:sqrt[sum[dist]] by meterid from lt;
    stdDistribution: select mean:avg[raze sqrt[sum[dist]]], stdev:sqrt[var[raze sqrt[sum[dist]]]] by meterid from t;
  /    .Q.gc[];
 /	stdDistribution:(`meterid) xkey normalDist each () xkey l2dist;
    `:/home/x362liu/kdb/stdDistribution set stdDistribution;
	};

/ -----------Bellow: The detection process ------------
computeDistance:{[row] 
    readings:row[`reading]; 
    temperatures:row[`temperature];  
    beta:row[`beta]; 
    ypred:beta[0]; 
    p:(count readings)-1; 
    i:0; 
    do[p; 
        i:i+1;  
        ypred:ypred+ beta[i]* readings[i]
        ]; 
    predictReading:ypred + (beta[i+1]*XT1 temperatures[0]) + (beta[i+2]*XT1 temperatures[0]) + (beta[i+3]*XT3 temperatures[0]); 
    :`meterid`hour`distance!(row[`meterid];row[`hour];(predictReading-readings[0])*(predictReading-readings[0]))
    };

propability:{[thedate;row] 
    threshold:0.0001;  
    pi:3.14159; 
    prop:exp[-1*((row[`distance]-row[`mean]) xexp 2) % (2*(row[`stdev] xexp 2))] % (row[`stdev]*sqrt[2*pi]);  
    if[prop<threshold; insert[`result](row[`meterid];thedate;prop)];
    };

detect:{[tstable;today] 
    p:3; 
    fname:`$"" sv(":/home/x362liu/datasets/parx/data5000/";string today;".csv");
    dataOfToday:flip `meterid`readtime`reading`temperature!("IZFF"; "|")0:fname; 
    tstable:select from tstable where readtime.date>=today-p; 
    tstable:`meterid`readtime xdesc tstable,dataOfToday;
    .Q.gc[];
    readingOfPPrevDays:select reading,temperature by meterid,hour:readtime.hh from tstable; 
    distances: select sqrt sum distance by meterid from computeDistance each () xkey parameters ij readingOfPPrevDays; 
    today propability/: () xkey distances ij stdDistribution;
    };

/ Directly load into memory
/ tstable:flip `meterid`readtime`reading`temperature!("IZFF"; "|")0:`:/home/xiuli/workspace/benchmark/data/trainingdata.csv;
/ meterIDs:distinct tstable[`meterid];
/ Load into file on disk


/ .Q.fs[{`:/home/x362liu/kdb/tstable upsert flip `meterid`readtime`reading`temperature!("IZFF"; "|")0:x}]`:/home/x362liu/datasets/data5000/train.csv;
/ tstable: value `:/home/x362liu/kdb/tstable;


cmd:.Q.opt .z.x;
startDate:2012.06.01;
endDate:("D"$cmd[`enddate])[0];
op:("I"$cmd[`op])[0];
dates:startDate + til 1+endDate-startDate;

tstable:flip `meterid`readtime`reading`temperature!("IZFF"; "|")0:`:/home/x362liu/datasets/parx/data5000/train.csv;

st:.z.T;
if[op=2; 
	tstable: select from tstable where readtime.date>2012.05.28;
	.Q.gc[]; 
	parameters: get `:/home/x362liu/kdb/parameters;
	stdDistribution: get `:/home/x362liu/kdb/stdDistribution
	];

/ $[op=1;train; tstable detect/:  dates];

$[op=1; train[tstable]; tstable detect/: dates];

save `:/home/x362liu/kdb/result.csv;
ed:.z.T;
show (ed-st); 


\\
