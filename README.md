# msd-metdisease-database-pmid-cid-builder

## msd command line

```
spark-submit \
   --deploy-mode cluster \
   --class fr.inrae.msd.rdf.PmidCidBuilder \
   --executor-memory 1G \
   --num-executors 10 \
   --conf spark.yarn.appMasterEnv.JAVA_HOME="/usr/local/openjdk/jdk-12.0.2+10/" \
   --conf spark.executorEnv.JAVA_HOME="/usr/local/openjdk/jdk-12.0.2+10/" \
   --jars /usr/share/java/sansa-stack-spark_2.12-0.8.4_ExDistAD.jar \
    msd-metdisease-database-pmid-cid-builder.jar
```

### Info

Void
https://ftp.ncbi.nlm.nih.gov/pubchem/RDF/void.ttl


https://ftp.ncbi.nlm.nih.gov/pubchem/RDF/reference/*_type*.ttl.gz
https://ftp.ncbi.nlm.nih.gov/pubchem/RDF/reference/pc_reference_type.ttl.gz

## Original command

``` 
source env/bin/activate
python3 -u app/build/import_PMID_CID.py --config="./config/release-2021/import_PMID_CID.ini" --out="./share-virtuoso" --log="./logs-app"
```
