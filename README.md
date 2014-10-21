Benchmark of Smart Meter Data Analytics
==============

We select the following five representative technologies for the benchmarking, including

* **Matlab** -- A traditional analytics tool;

* **MADlib** -- In-database (PostgreSQL) analytics library;

* **System C** -- In-memory column store;

* **Spark** -- Main memory based distributed computing framework;

* **Hive** -- A distributed data warehouse system on Hadoop;


*Matlab, MADlib*, and *System C* are benchmarked in a centralized environment, e.g., on a single server, while *Spark* and *Hive* are benchmarked in a distributed environment, e.g., multiple clustered servers.

We select the following four representative smart meter data analytics algorithms for the benchmarking, including:

* **3-Line** -- The model of using three linear regression lines to fit the relationship between meter readings and weather temperatures, (see the paper [])

* **PAR** -- In-database (PostgreSQL) analytics library;

* **Histogram** -- In-memory column store;
 
* **Cosine similarity** -- In-memory column store;
* 

#Reference
[1] O. Ardakanian, N. Koochakzadeh, R. P. Singh, L. Golab, and S.Keshav, Computing Electricity Consumption Profiles from Household Smart Meter Data, in EnDM Workshop on Energy Data Management, pp.140-147, 2014.
