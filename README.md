# Spark watersheds pipeline

## Compile for Janelia spark cluster
```bash
mvn -Pfat clean package 
cp target/watersheds-spark-<version>-shaded.jar /location/on/the/cluster
```
