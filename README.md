# Spark Watersheds Pipeline

## Compile for Janelia spark cluster
```bash
mvn -Pfat clean package 
cp target/watersheds-spark-<version>-shaded.jar /location/on/the/cluster
```

## Example of Usage
```bash
N_NODES=${N_NODES:-10}
N_EXECUTORS_PER_NODE=${N_EXECUTORS_PER_NODE:-15}

# N5 group that holds distance transform and results
N5_GROUP="$HOME/from_heinrichl/fib25_sub_prediction_at_296000.n5"

# group for output (optional, defaults to ${N5_GROUP})
N5_GROUP_OUTPUT=${N5_GROUP}

# group for intermediate results during block merging (optional, defaults to ${N5_GROUP_OUTPUT})
TMP_GROUP=${N5_GROUP_OUTPUT}/tmp

# input dataset
DATASET="data"

# N5 dataset for result (optional, defaults to 'supervoxels')
TARGET="supervoxels"

# N5 dataset for result merged across blocks (optional, defaults to ${TARGET}-merged)
TARGET_MERGED="supervoxels-merged"

# threshold for seeding (optional, no thresholding if not specified)
THRESHOLD=0.5

# block size of watershed. I recommend WATERSHED_BLOCKSIZE to be integer multiple of dt block size
WATERSHED_BLOCKSIZE=100,100,100

# CONTEXT for watersheds (optional, defaults to 1)
CONTEXT=10

# min val to be considered for distances (optional, defaults to {0,1})
HIST_MIN=-1.0
HIST_MAX=2.0


JAR="$HOME/watersheds-spark-0.0.1-SNAPSHOT-shaded.jar"
CLASS="org.saalfeldlab.watersheds.Watersheds"
# BLOCKSIZE=1448,1529,1

# affinities or relief (currently only relief), required
WATERSHED_TYPE=relief

TERMINATE=1 SPARK_VERSION=2 N_EXECUTORS_PER_NODE=$N_EXECUTORS_PER_NODE \
         $HOME/flintstone/flintstone.sh ${N_NODES} $JAR $CLASS \
         $WATERSHED_TYPE \
         -b $WATERSHED_BLOCKSIZE \
         -H $CONTEXT \
         -i \
         -m $HIST_MIN \
         -M $HIST_MAX \
         --merge-blocks \
         --output-group $N5_GROUP_OUTPUT \
         --tmp-group $TMP_GROUP \
         --watersheds-dataset $TARGET \
         --watersheds-merged-dataset $TARGET_MERGED \
         -t $THRESHOLD \
         $N5_GROUP \
         $DATASET
```
