# Spark watersheds pipeline

## Compile for Janelia spark cluster
```bash
mvn -Pfat clean package 
cp target/imglib2-algorithm-watershed-examples-spark-<version>-shaded.jar /location/on/the/cluster
```
