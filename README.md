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
   --conf spark.yarn.submit.waitAppCompletion="false" \
   --jars /usr/share/java/sansa-stack-spark_2.12-0.8.4_ExDistAD.jar \
    msd-metdisease-database-pmid-cid-builder.jar
```

### Model 

[![](https://mermaid.ink/img/pako:eNptkd1O40AMRl_FmqtUavMAvahUOs2CtLugtoiLpkKzGaf1NskEj8NveXcmCYiAuBuNfI792S8qcxbVVO3Z1AfY6LQCmG9_kxdwOTDmyFhlCCudQE4F7mAymZ1Stca77VqYqv0uVXCCs2iPcvXnQrdowq5cfaCjVnn2jq20_sROi4ixNiwk5CqoTXa89fTcE4sfCdDR8lqo8DEWVB1hMoO6JLsgcZp81niPfkG2M-iBIeoV48u6bbUdTL8bdd7lNqFCkHctufyBHCA9kURXXWd74_gY_2uo-DrG6LtpkOJX9MAkqI2YnE2Jc795FGjXBw8kB-jDIbPjTpMMNI73sQm7OmD8HysTd5eLN0x1uE4rP4-8uQ_Kv_2fD8Gw29JtRjYWKUZqrErk0pANl39pG6RKDlhiqqbhaTE3TSGpSqvXUNrU1ggubQjHapqbwuNYmUbc-qnK1FS4wY8iTSaMU75Xvb4Bp83OHA)](https://mermaid-js.github.io/mermaid-live-editor/edit#pako:eNptkd1O40AMRl_FmqtUavMAvahUOs2CtLugtoiLpkKzGaf1NskEj8NveXcmCYiAuBuNfI792S8qcxbVVO3Z1AfY6LQCmG9_kxdwOTDmyFhlCCudQE4F7mAymZ1Stca77VqYqv0uVXCCs2iPcvXnQrdowq5cfaCjVnn2jq20_sROi4ixNiwk5CqoTXa89fTcE4sfCdDR8lqo8DEWVB1hMoO6JLsgcZp81niPfkG2M-iBIeoV48u6bbUdTL8bdd7lNqFCkHctufyBHCA9kURXXWd74_gY_2uo-DrG6LtpkOJX9MAkqI2YnE2Jc795FGjXBw8kB-jDIbPjTpMMNI73sQm7OmD8HysTd5eLN0x1uE4rP4-8uQ_Kv_2fD8Gw29JtRjYWKUZqrErk0pANl39pG6RKDlhiqqbhaTE3TSGpSqvXUNrU1ggubQjHapqbwuNYmUbc-qnK1FS4wY8iTSaMU75Xvb4Bp83OHA)

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
