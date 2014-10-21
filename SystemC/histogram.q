round:{floor x+0.5};
range:{(min x;max x)};
hist:{[x;nbins] count each group (abs (-) . range x % nbins) xbar (asc x) };

histmain:{[homeid];
    data:select readtime,reading from essex where int=homeid; // In-memory
    data:flip `readtime`reading!(raze data[`readtime]; raze data[`reading]); // In-mem
    readings:select reading from data;
    //readings:flip select reading from essex where int=homeid; //From db
    :hist[readings[`reading];10];
 };

homeids:("II";",") 0: `:/home/x362liu/datasets/homeids.csv;
homeids:homeids[0];

// Test when all the data is kept in main memory
// essex:flip `int`readdate`readtime`reading`temperature!("IDIFF"; ",")0:`:/home/x362liu/datasets/syn10y/data.csv;
// essex:select reading, readtime by int from essex;

\l /home/x362liu/kdb/db

st:.z.T;
histmain peach homeids;
ed:.z.T;
show "Time=";
show ed-st;

\\

