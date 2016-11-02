# KAFKA_CARDINALITY 

Count number of distinct values (cardinality) in Kafka stream for a given JSON key. 

Algorithms used: HashSet, HyperLogLog, Linear counting

Details in [doc/data_engineer_task.doc](https://github.com/svetaj/KAFKA_CHALLENGE/blob/master/doc/data_engineer_task.pdf)

## Kafka installation  

     tar -xzf kafka_2.11-0.10.0.0.tgz

     cd kafka_2.11-0.10.0.0 

     bin/zookeeper-server-start.sh config/zookeeper.properties

     bin/kafka-server-start.sh config/server.properties

## Kafka topic creation

     bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic streams-file-input

     bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic tamedia

## Sending test data to Kafka topic using Kafka producer

     cat file-input.txt | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic streams-file-input

     zcat stream.jsonl.gz | head -1000 | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic tamedia

## How to compile/run Java programs

Use Maven to build:

    pom.xml contains all dependancies

    download this repository to local empty directory

    run "mvn package" to build

    run "deploy.sh" to prepare for execution

    export CLASSPATH displayed by deploy.sh, fix Kafka jar location if necessary

    run examples 

### create a small app 

Reads this data from kafka and prints it to stdout, send topic items to stdout, parse JSON (extract key,value)

    KafkaPipe.java (explained later)

## Algorithms for counting distinct elements in a stream 

HashSet, HyperLogLog, Linear counting

Details in [big-data-counting-how-to-count-a-billion-distinct-objects](http://highscalability.com/blog/2012/4/5/big-data-counting-how-to-count-a-billion-distinct-objects-us.html)


### Cardinality computation of values for a given JSON key

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

## Benchmark 

Not implemented in this version. 

Compare HashSet, HyperLogLog, Linear counting. Memory consumption, precision, execution duration.

## Output to a new Kafka Topic instead of stdout

The key idea is to produce one minute estimators (serialized Linear counting bitmap):

![alt tag](https://github.com/svetaj/KAFKA_CHALLENGE/blob/master/estimator.jpg)

DataEstimator.java - produces estimator from input stream (there are some thread synchronization issues)

EstimatorSum.java - not implemented yet

TEST ESTIMATOR GENERATION (source and destination can be topic or stdin)

    bin/kafka-topics.sh --delete --zookeeper localhost:2181 --topic min_est

    bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic min_est
    
    cat /tmp/stream-count/streamx.jsonl | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic jsonxx
    
    bin/kafka-run-class.sh DataEstimator jsonxx min_est
    
    bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic min_est  --from-beginning

    
    cat /tmp/stream-count/streamx.jsonl | java DataEstimator jsonxx min_est

TEST ONE MINUTE GENERATION 

(provide sufficient number of bytes to avoid overflow, in this example is set to 100)

    # terminal session 1:
    
    for x in 1 10000 20000 30000 40000 50000 60000 70000 80000
    
    do
    
         zcat stream.jsonl.gz | head -$x | tail -1000 | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic jsonxx
         
         sleep 50
    
    done
    
    # terminal session 2:
    
    bin/kafka-run-class.sh DataEstimator jsonxx min_est 100 60
    
    # terminal session 3:
    
    bin/kafka-run-class.sh EstimatorSum min_est hour_est 100 3600
    
    # terminal session 4:
    
    bin/kafka-run-class.sh GetEstimator min_est
    
    # view topic stream (jsonxx, min_est, hour_est)
    
    bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic TOPIC_NAME --from-beginning


    # stdin i stdout instead of kafka
    
    cat streamx.jsonl  | java DataEstimator stdin stdout 12 60    > min_est.jsonl
    
    cat min_est.jsonl  | java EstimatorSum  stdin stdout 100 3600 > hour_est.jsonl
    
    cat min_est.jsonl  | java GetEstimator  stdin
    
    cat hour_est.jsonl | java GetEstimator  stdin

## Performance measurement and optimization

Related to expected cardinality and proper setting of HyperLogLog or Linear counting parameter.

Details in [doc/data_engineer_work.doc](https://github.com/svetaj/KAFKA_CHALLENGE/blob/master/doc/data_engineer_work.pdf)

## Scalability

Based on 1 minute estimators and Json object (estimator is serialized bitmap): 

    {“ts”:<timestamp>, “range“:<range>,”ec”:<ecvalue>,”est”:<estimator>}

Details in [doc/data_engineer_work.doc](https://github.com/svetaj/KAFKA_CHALLENGE/blob/master/doc/data_engineer_work.pdf)

## Edge cases, options and other things

Details in [doc/data_engineer_work.doc](https://github.com/svetaj/KAFKA_CHALLENGE/blob/master/doc/data_engineer_work.pdf)



