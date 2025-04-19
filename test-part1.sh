#!/bin/bash

# Start Hadoop and Spark services
./start.sh
source ./env.sh
/usr/local/hadoop/bin/hdfs dfsadmin -safemode leave
echo "Removing existing HDFS input directory..."
/usr/local/hadoop/bin/hdfs dfs -rm -r /lab2p1/input/ || echo "No existing directory to remove."

# Create new HDFS input directory
echo "Creating new HDFS input directory..."
/usr/local/hadoop/bin/hdfs dfs -mkdir -p /lab2p1/input/

# Copy data from local to HDFS input directory
echo "Copying data from local to HDFS..."
/usr/local/hadoop/bin/hdfs dfs -copyFromLocal parking2024.csv /lab2p1/input/

# Set the parallelism level
if [ "$#" -eq 1 ]; then
    parallelism_parameter=$1
else
    parallelism_parameter=2  # Default parallelism parameter
fi

echo "Running Spark job with parallelism level equals $parallelism_parameter..."
/usr/local/spark/bin/spark-submit --master=spark://$SPARK_MASTER:7077 \
    --conf spark.default.parallelism=$parallelism_parameter \ 
    ./part1.py hdfs://$SPARK_MASTER:9000/lab2p1/input/parking2024.csv $parallelism_parameter

# Stop Hadoop and Spark services
./stop.sh
