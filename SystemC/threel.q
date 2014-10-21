round:{floor x+0.5};
range:{(min x;max x)};
quantile:{[x;p]  xs:asc distinct x; n:count xs; 0.5*sum xs (floor n*p;1+floor n*p)};
summary:{ `min`1q`median`mean`3q`max!(min x;quantile[x;.25];med x;avg x;quantile[x;.75];max x) };
hist:{[x;nbins] count each group (abs (-) . range x % nbins) xbar (asc x) };
linregr:{[y;X]if[any[null y:"f"$y]|any{any null x}'[X:"f"$X];'`nulls]; if[$[0=m:count X;1;m>n:count X:flip X];'`length]; Z:inv[flip[X]mmu X];ZZ:X mmu Z mmu flip[X]; e:y- yhat:X mmu beta:Z mmu flip[X] mmu y;``S`beta`e`n`m`df`ZZ`Z`yhat!(::;Z*mmu[e;e]%n-m;beta;e;n;m;n-m;ZZ;Z;yhat)};


SME:{[rec; x0; y0; x1; y1];
    n:count rec; m:0;
    v:0;
    do[n;
		v:v+((y0+((y1-y0)%(x1-x0))*(rec[m][`tem]-x0))-rec[m][`qua]) xexp 2;
		m:m+1;
     ];
     
     $[n>0; sqrt v%n; 0]
 };

conlines:{[gridsize;t;temp]; 
   y0:t[`beta1][0]+t[`beta1][1]*t[`t0];
   y11:t[`beta1][0]+t[`beta1][1]*t[`t1];
   y21:t[`beta2][0]+t[`beta2][1]*t[`t1];
   y22:t[`beta2][0]+t[`beta2][1]*t[`t2];
   y32:t[`beta3][0]+t[`beta3][1]*t[`t2];
   yn:t[`beta3][0]+t[`beta3][1]*t[`tn];
   sub:{[st;ed;temp] select tem, qua from temp where tem within(st;ed)};
   
   pts1:();
   pts2:();
   r:0; 
   do[gridsize;
     c:0;
     do[gridsize;
        pts1:pts1,enlist (t[`t1]+0.5*(c-(floor gridsize%2)); ((y11+y21)%2)+((abs (y21-y11))%4)*((floor gridsize%2)-r));
        pts2:pts2,enlist (t[`t2]+0.5*(c-(floor gridsize%2)); ((y22+y32)%2)+((abs (y32-y22))%4)*((floor gridsize%2)-r));
        c:c+1;
        ];
       r:r+1;
     ];
       
    op2:();
    op1:();
    i:0;
    minSME:0W;
    do[gridsize;
      p1:pts1[i];

      SME1:SME[sub[t[`t0]; p1[0];temp]; t[`t0]; y0; p1[0]; p1[1]];

      j:0;
      do[gridsize;
	    p2:pts2[j];
	     SME23:SME[sub[p1[0];p2[0];temp]; p1[0];p1[1];p2[0];p2[1]]+SME[sub[p2[0];t[`tn];temp];p2[0];p2[1];t[`tn];yn];
	     if[(SME1+SME23)<minSME; minSME:(SME1+SME23); op1:p1; op2:p2];
	    j:j+1;
        ];
       
      i:i+1;
      ]; 

	$[(count op1) and (count op2); 	
    :`homeid`x1`y1`x2`y2`slope1`slope2`slope3!(t[`homeid]; op1[0]; op1[1]; op2[0]; op2[1]; (op1[1]-y0)%(op1[0]-t[`t0]); (op2[1]-op1[1])%(op2[0]-op1[0]); (yn-p2[1])%(t[`tn]-p2[0]));
    :0N]
 };

quantileload:{[hid; essexdata; qvalue];
			// temp:select qua: quantile[reading[0]; qvalue] by tem from (select cnt:count reading, reading by tem:round temperature from essex where homeid=hid) where cnt>20;  
            temp: select qua:quantile[reading; qvalue] by tem:round[temperature] from  essexdata; // flip essex[hid];
			optErr:0W;
			optT1:99;
			optT2:99;
            t1:10;
			a:beta1:();
			b:beta2:();
			c:beta3:();
			do[10;
				t1Err:99;
				qt:select qua, tem from temp where tem<=t1;	
				if[(count qt)>1; r:linregr[qt[`qua];(1.0;qt[`tem])]; t1Err:avg r[`e]; beta1:r[`beta]];
				
				t2:t1+5;
				do[5;
				  t2Err:99;
				  qt:select qua, tem from temp where tem>=t1,tem<=t2;
				  
				  if[(count qt)>1; r:linregr[qt[`qua];(1.0;qt[`tem])]; t2Err:avg r[`e];beta2:r[`beta]];

				  t3Err:99;
				  qt:select qua, tem from temp where tem>=t2;
				  if[(count qt)>1; r:linregr[qt[`qua];(1.0;qt[`tem])]; t3Err:avg r[`e]; beta3:r[`beta]];
				  
				  err:t1Err+t2Err+t3Err;
				  if[err<optErr; optErr:err; optT1:t1; optT2:t2; a:beta1; b:beta2; c:beta3];
				  t2:t2+1;
				 ];     
				 t1:t1+1;
			 ];

			if [(0=count a)|(0=count b)|(0=count c);:0N];
			
			param:`homeid`t0`t1`t2`tn`beta1`beta2`beta3!(hid;first (key temp)[`tem];t1;t2;last (key temp)[`tem];(a[0];a[1]);(b[0];b[1]);(c[0];c[1]));
			conlines[3; param; temp]   
   };

pthreeline:{[homeid]
         data:select reading, temperature from essex where int=homeid;
         // data:flip `reading`temperature!(raze data[`reading]; raze data[`temperature]); //Test when all the data is kept in memory
		 base:quantileload[homeid; data; 0.1];
		 active:quantileload[homeid; data; 0.9];		
		if[(99h=type base) and (99h=type active);
			baseLoad:min (base[`y1]; base[`y2]);
			active1:min (active[`y1]; active[`y2]);
			activeLoad: (active1 - baseLoad);
			heatingGrad:active[`slope1];
			chp:active[`x2];
			coolingGrad:active[`slope3];
			if[active[`slope2]>active[`slope3];chp:active[`x1];coolingGrad:active[`slope2]];
			:(homeid;baseLoad;activeLoad;`float$chp;heatingGrad;coolingGrad);
		]};
   
threelmain:{
    threetbl:([]homeid:`int$(); baseload:`float$(); activeload:`float$(); ac:`float$(); heatinggrad:`float$(); coolinggrad:`float$());
    st:.z.T;
	t: pthreeline peach homeids;
    i:0;
    do[count t;
        if[6=count t[i]; threetbl,:t[i]];
		i:i+1;
	  ];
    `:/home/x362liu/kdb/threelresults.csv 0:.h.tx[`csv;threetbl];
	ed:.z.T;
	show (ed-st); 
 };


homeids:("II";",") 0: `:/home/x362liu/datasets/homeids.csv;
homeids:homeids[0];

// Test when all the data is kept in main memory
// essex:flip `int`readdate`readtime`reading`temperature!("IDIFF"; ",")0:`:/home/x362liu/datasets/syn10y/data.csv;
// essex:select reading, temperature by int from essex;

\l /home/x362liu/kdb/db



threelmain[];
\\

