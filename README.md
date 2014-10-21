Benchmark of Smart Meter Data Analytics
==============

We select the following five representative technologies for the benchmarking, including

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
-------------
To use this benchmark, users could use this [data generator](https://github.com/xiufengliu/SmartMeterDataGenerator) to generate smart meter time series data.

# Installation and Usage
---------------------



#Reference
-----------------
[1] B. J. Birt, G. R. Newsham, I. Beausoleil-Morrison, M. M. Armstrong, N. Saldanha, and I. H. Rowlands, Disaggregating
Categories of Electrical Energy End-use from Whole-house Hourly Data, Energy and Buildings, 50:93-102, 2012.

[2] O. Ardakanian, N. Koochakzadeh, R. P. Singh, L. Golab, and S.Keshav, Computing Electricity Consumption Profiles from Household Smart Meter Data, in EnDM Workshop on Energy Data Management, pp.140-147, 2014.
