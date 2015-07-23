result:([]meterid:();readdate:();propability:());

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

/ -------------- Training process --------------
train:{
    tstable:flip `meterid`readtime`reading`temperature!("IZFF"; "|")0:`:/home/x362liu/datasets/neighbors/train.csv;
    neighbors:flip `mymeterid`meterid!("II"; "|")0:`:/home/x362liu/datasets/neighbors/neighbors25000.csv;
    meterids: `meterid xkey select distinct meterid:mymeterid,0N from  neighbors;
    neighborDailyAvg:select neighborAvg:avg reading by meterid:mymeterid, readdate:readtime.date, hour:readtime.hh from tstable ij `meterid xkey neighbors;
    myDailyAvg: select mine:avg reading by meterid, readdate:readtime.date,hour:readtime.hh from tstable ij meterids;
    dailyL2Distance:select l2distance:(sqrt sum (mine-neighborAvg) xexp 2) by meterid, readdate from myDailyAvg ij neighborDailyAvg;
    stdDistribution:select mean:avg[l2distance], stdev:sqrt[var[l2distance]] by meterid from dailyL2Distance;
    `:/home/x362liu/kdb/neighbormodel set stdDistribution;
    };

/ ------------ Detection process ---------------

detect:{[today]
    stdDistribution: get `:/home/x362liu/kdb/neighbormodel;
    neighbors:flip `mymeterid`meterid!("II"; "|")0:`:/home/x362liu/datasets/neighbors/neighbors20000.csv;
    meterids: `meterid xkey select distinct meterid:mymeterid,0N from  neighbors;
    fname:`$"" sv(":/home/x362liu/datasets/neighbors/";string today;".csv");
    dataOfToday:flip `meterid`readtime`reading`temperature!("IZFF"; "|")0:fname;
    neighborAvgOfToday:select neighborAvg:avg reading by meterid:mymeterid, hour:readtime.hh from dataOfToday ij `meterid xkey neighbors;
    myAvgOfToday: select mine:avg reading by meterid,hour:readtime.hh from dataOfToday ij meterids;
    l2DistanceOfToday:select l2distance:(sqrt sum (mine-neighborAvg) xexp 2) by meterid from myAvgOfToday ij neighborAvgOfToday;
    threshold:0.01;
    pi:3.14159;
    propTable:select meterid, readdate:today, propability:exp[-1.0*(l2distance-mean) % (2*stdev*stdev)] % (stdev*sqrt[2*pi]) from l2DistanceOfToday ij stdDistribution;
    `result insert select from propTable where propability<threshold;
    };

/ result:([]meterid:"i"$();readdate:"d"$();propability:"f"$());

cmd:.Q.opt .z.x;
st:.z.T;
startDate:2012.06.01;
endDate:("D"$cmd[`enddate])[0];
op:("I"$cmd[`op])[0];
dates:startDate + til 1+endDate-startDate;
/ train[];
$[op=1;train[]; detect each dates];
save `:/home/x362liu/kdb/result.csv;
ed:.z.T;

show (ed-st);
\\
