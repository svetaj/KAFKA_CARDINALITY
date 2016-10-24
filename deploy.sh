#!/bin/sh

export KAFKA=/usr/local/kafka/kafka_2.11-0.10.0.0
DIR=`pwd`
cd /tmp
mkdir stream-count
cd stream-count
wget http://central.maven.org/maven2/com/googlecode/json-simple/json-simple/1.1/json-simple-1.1.jar
wget http://central.maven.org/maven2/com/clearspring/analytics/stream/2.9.5/stream-2.9.5.jar
cp $DIR/target/stream-count-1.0-SNAPSHOT.jar stream-count.jar
cp $DIR/tst/streamx.jsonl .
export CLASSPATH="/tmp/stream-count/*:$KAFKA/libs/*"
echo "Current CLASSPATH is "$CLASSPATH
[ ! -f $KAFKA/libs/kafka-streams-0*.jar ] &&  echo "set CLASSPATH to proper Kafka location"
echo ; echo ; echo
cat << !END!
USAGE:
  bin/kafka-run-class.sh KafkaPipe kafka_topic_name json_key_name [HASHSET] [LOGLOG] [LINEAR]

EXAMPLES:
  export CLASSPATH="$CLASSPATH"
  cat /tmp/stream-count/streamx.jsonl | jq .uid | sort -u | wc -l 
  cd $KAFKA
  bin/kafka-topics.sh --list --zookeeper localhost:2181
  bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic test1
  bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic jsonxx 
  cat /tmp/stream-count/streamx.jsonl | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic jsonxx

  bin/kafka-run-class.sh KafkaPipe jsonxx uid LINEAR LOGLOG HASHSET

  cat /tmp/stream-count/streamx.jsonl | java KafkaPipe stdin uid LINEAR LOGLOG HASHSET
!END!
