# Spark Project Skeleton


Skeleton project for Spark

## Prerequisites

## Usage

##### Install

```sh
sbt compile
```

##### Packaging

```sh
sbt assembly
```

##### Test

```sh
sbt test
```


#### Command

```
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master mesos://207.184.161.138:7077 \
  --deploy-mode cluster \
  --supervise \
  --executor-memory 2G \
  --total-executor-cores 2 \
  ----num-executors 3 \
  http://path/to/examples.jar \
  1000
```

