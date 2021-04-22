# Lambda Architecture for Twitter Realtime Image Processing

### Description 
Lambda Architecture implementation using Apache Storm, Hadoop and HBase to perform Twitter real-time image processing analysis.
The goal of this project is find the most representative images from images obtained from Twitter through a certain keyword.\
To find these representative images, we will use the K-Means algorithm.

Link to the whole [paper](Parallel_Computing_Main_final.pdf)

### Dependencies
  * [Apache Hadoop-3.2.1](https://hadoop.apache.org/)
  * [Apache HBase-2.2.3](https://hbase.apache.org/)
  * [Apache Storm-2.1.0](https://storm.apache.org/)
  * [Twitter4j-4.0.4](http://twitter4j.org/en/)
  * [Lire ( Lucene Image Retrieval )-8.0.0](https://github.com/dermotte/LIRE)
  * [Gradle-6.3](https://gradle.org/)

### Pre-requisites
Hbase, Storm and Hadoop have to be installed and set correctly on your pseudo-distribuited cluster.

### Usage
To collect the images from Twitter, you have to get your personal Twitter Developers credential and insert it on .txt file.
You will find an example inside the project as FakeCredential.txt

Start in this order:
* Hadoop
* HBase
* Storm ( it start automatically on Eclipse in my case ) 

After that, you can finally execute in this order:
* `TwitterRealTimeImageProcessing.java`
* `HadoopDriver.java`
