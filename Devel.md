
## Install Spark/Hadoop 

- Hadoop : [3.3.0](https://archive.apache.org/dist/hadoop/common/hadoop-3.3.0/hadoop-3.3.0.tar.gz) 
- Spark : [3.1.2](https://dlcdn.apache.org/spark/spark-3.1.3/spark-3.1.3-bin-hadoop3.2.tgz) 

``` 
tar xvf 
sudo mv spark-3.1.3-bin-hadoop3.2 /usr/local/share/
sudo ln -s /usr/local/share/spark-3.1.3-bin-hadoop3.2 /usr/local/share/spark
```

``` 
tar xvf hadoop-3.3.0.tar.gz
sudo mv hadoop-3.3.0 /usr/local/share/
sudo ln -s /usr/local/share/hadoop-3.3.0 /usr/local/share/hadoop
``` 

### set xml config file 
https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html


```
export SPARK_HOME=/usr/local/share/spark
export HADOOP_HOME=/usr/local/share/hadoop
```

`/usr/local/share/spark/bin/spark-submit --class fr.inrae.msd.rdf.PmidCidBuilder --executor-memory 1G --num-executors 1 target/scala-2.12/msd-metdisease-database-pmid-cid-builder.jar --versionMsd "test" -d ./rdf`

## Msd Datalake
``` 
scp ./target/scala-2.12/msd-metdisease-database-pmid-cid-builder.jar ofilangi@ara-unh-elrond:~/
```
