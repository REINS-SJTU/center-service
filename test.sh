index=$1
enableRewrite=$2
mvName=$3
sql=$4

echo "arg:" $index
echo "arg:" $enableRewrite
echo "arg:" $mvName
echo "arg:" $sql

SPARK_SUBMIT=/Users/zzt/deploy/spark-3.1-bin-test6/bin/spark-submit
JAR=/Users/zzt/code/spark-to-delta/target/spark-to-delta-1.0-SNAPSHOT.jar
CLASS=com.reins.SparkTpchSQL

$SPARK_SUBMIT \
--packages org.apache.hudi:hudi-spark3-bundle_2.12:0.10.0,mysql:mysql-connector-java:8.0.16 \
--class $CLASS \
--master spark://localhost:7077 \
--deploy-mode cluster \
--executor-memory 4G \
--total-executor-cores 4 \
--driver-memory 4G \
--driver-cores 4 \
$JAR \
$index \
$enableRewrite \
$mvName \
"$sql"



