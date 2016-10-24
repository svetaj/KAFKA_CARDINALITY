# KAFKA_CHALLENGE 

https://github.com/tamediadigital/hiring-challenges/tree/master/data-engineer-challenge


## install kafka 

     tar -xzf kafka_2.11-0.10.0.0.tgz

     cd kafka_2.11-0.10.0.0 

     bin/zookeeper-server-start.sh config/zookeeper.properties

     bin/kafka-server-start.sh config/server.properties

## create a topic

     bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic streams-file-input

     bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic tamedia

## use the kafka producer from kafka itself to send our test data to your topic

     cat file-input.txt | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic streams-file-input

     zcat stream.jsonl.gz | head -1000 | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic tamedia

## How to compile/run

Use Maven to build:

    pom.xml contains all dependancies

    download this repository to local empty directory

    run "mvn package" to build

    run "deploy.sh" to prepare for execution

    export CLASSPATH displayed by deploy.sh, fix Kafka jar location if necessary

    run examples 


## create a small app that reads this data from kafka and prints it to stdout

### send topic items to stdout

    bin/kafka-run-class.sh MyCAT streams-file-input

### parse JSON (extract key,value)

    bin/kafka-run-class.sh MyJSON tamedia uid


## find a suitable data structure for counting and implement a simple counting mechanism, output the results to stdout 

    HashSet, HyperLogLog, Linear counting
    details in doc/data_engineer_work.doc
    
### compute cardinality of values for a given JSON key

For demonstration purposes KafkaPipe.java can be used. The program arguments are:
kafka_topic_name - if "stdin" it reads from stdin and don't require Kafka up and running.
json_key_name    - set to "uid"
method           - HASHSET or/and LOGLOG or/and LINEAR - we can provide min 1 max all 3 

USAGE:

    export CLASSPATH="/tmp/stream-count/*:/usr/local/kafka/kafka_2.11-0.10.0.0/libs/*
    bin/kafka-run-class.sh KafkaPipe kafka_topic_name json_key_name [HASHSET] [LOGLOG] [LINEAR]

EXAMPLES:

    export CLASSPATH="/tmp/stream-count/*:/usr/local/kafka/kafka_2.11-0.10.0.0/libs/*"
    cat /tmp/stream-count/streamx.jsonl | jq .uid | sort -u | wc -l 
    cd /usr/local/kafka/kafka_2.11-0.10.0.0
    bin/kafka-topics.sh --list --zookeeper localhost:2181
    bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic test1
    bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic jsonxx 
    cat /tmp/stream-count/streamx.jsonl | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic jsonxx

    bin/kafka-run-class.sh KafkaPipe jsonxx uid LINEAR LOGLOG HASHSET

    cat /tmp/stream-count/streamx.jsonl | java KafkaPipe stdin uid LINEAR LOGLOG HASHSET


# advanced solution

## benchmark

Not implemented in this version. 

Compare HashSet, HyperLogLog, Linear counting. Memory consumption, precision, execution duration.

## Output to a new Kafka Topic instead of stdout

Not implemented in this version. 

PipeDemo.java explains how to copy from one topic to another without filtering.

        KStreamBuilder builder = new KStreamBuilder();
        
        builder.stream("streams-file-input").to("streams-pipe-output");

WordCountDemo.java uses KTable to filter stream and to materialize in another stream (topic) 

        KTable<String, Long> counts = ...
        
        counts.to(Serdes.String(), Serdes.Long(), "streams-wordcount-output");
        
## try to measure performance and optimize

Related to expected cardinality and proper setting of HyperLogLog or Linear counting parameter.

Details in doc/data_engineer_work.doc

## write about how you could scale

Not implemented in this version. 

Based on 1 minute estimators and Json object {“ts”:<timestamp>, “range“:<range>,”ec”:<ecvalue>,”est”:<estimator>}

Details in doc/data_engineer_work.doc

## only now think about the edge cases, options and other things

    details in doc/data_engineer_work.doc



