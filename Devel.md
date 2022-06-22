
## Install Spark

- Spark : [3.1.2](https://dlcdn.apache.org/spark/spark-3.1.3/spark-3.1.3-bin-hadoop3.2.tgz) 

``` 
tar xvf 
sudo mv spark-3.1.3-bin-hadoop3.2 /usr/local/share/
sudo ln -s /usr/local/share/spark-3.1.3-bin-hadoop3.2 /usr/local/share/spark
```

## addd Sansa with spark-submit Dependencies

### local 

```sh
/usr/local/share/spark/bin/spark-submit \
   --class fr.inrae.msd.rdf.PmidCidBuilder \
   --executor-memory 1G \
   --num-executors 1 \
   --jars ./sansa-ml-spark_2.12-0.8.0-RC3-SNAPSHOT-jar-with-dependencies.jar \
    assembly/msd-metdisease-database-pmid-cid-builder.jar -d ./rdf
```

## elrond

```sh
/usr/local/share/spark/bin/spark-submit \
   --class fr.inrae.msd.rdf.PmidCidBuilder \
   --executor-memory 1G \
   --num-executors 10 \
   --jars ./sansa-ml-spark_2.12-0.8.0-RC3-SNAPSHOT-jar-with-dependencies.jar \
    ./msd-metdisease-database-pmid-cid-builder.jar -d ./rdf
```

## Msd Datalake
``` 
scp ./target/scala-2.12/msd-metdisease-database-pmid-cid-builder.jar ofilangi@ara-unh-elrond:~/
```
