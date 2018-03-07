Benchmark of Smart Meter Data Analytics
==============

In this benchmark, we select the following five representative technologies for the benchmarking, including

* **Matlab** -- A traditional analytics tool;

* **MADlib** -- In-database (PostgreSQL) analytics library;

* **System C** -- In-memory column store (the name is omitted due to license issue);

* **Spark** -- Main memory based distributed computing framework;

* **Hive** -- A distributed data warehouse system on Hadoop;


*Matlab, MADlib*, and *System C* are benchmarked in a centralized environment, e.g., on a single server, while *Spark* and *Hive* are benchmarked in a distributed environment, e.g., multiple clustered servers.

We select the following four representative smart meter data analytics algorithms for the benchmarking, including:

* **3-Line** -- which is the model of using three linear regression lines to fit the relationship between meter readings and weather temperatures (see [1]);

* **PAR** -- which is a periodic auto-regression model for  extracting daily consumption trends that occur regardless of the outdoor temperature (see [2]);

* **Histogram** -- which is used to  understand the consumption variability of an energy consumer;

* **Cosine similarity** -- which is used to find groups of similar consumers, e.g., according to energy consumption;
 
# Synthetic Data sets
To use this benchmark, users could use this [data generator](https://github.com/xiufengliu/SmartMeterDataGenerator) to generate smart meter time series data.

# Installation and Usage

The implementation programming languages are MATLAB for Matlab, plpgsql for MADlib, Q for System C, and Java for Spark and Hive. Following are the installation guidline:

* *MADlib*: [MADlib](http://madlib.net/) library has first to be installed in PostgreSQL, then create the tables using the script for storing the time time series data, and finally install the functions of the benchmarking algorithms.

* *Spark and Hive*: Use the maven command *mvn package* to compile the Java program, and pack into a jar library.

Go to the corresponding folder of each technology, and execute *./run.sh* for running the algorithms. Note: *the run.sh shell scripts needs to be customized according to user's sepcific settings.*

Publication
========================

# Reference
[1] B. J. Birt, G. R. Newsham, I. Beausoleil-Morrison, M. M. Armstrong, N. Saldanha, and I. H. Rowlands, Disaggregating
Categories of Electrical Energy End-use from Whole-house Hourly Data, Energy and Buildings, 50:93-102, 2012.

[2] O. Ardakanian, N. Koochakzadeh, R. P. Singh, L. Golab, and S.Keshav, Computing Electricity Consumption Profiles from Household Smart Meter Data, in EnDM Workshop on Energy Data Management, pp.140-147, 2014.

[3] M. Arlitt, et al. [IoTAbench: an Internet of Things Analytics benchmark](http://www.hpl.hp.com/techreports/2014/HPL-2014-75.pdf). In Proc. of ICPE, 2015.
