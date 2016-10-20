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

exported variables:

    $MYSRC      (location where are my java sources)

    $KAFKA      (location where is kafka installed - contains bin directory)

    $JEE        (location where is unzipped J2EE)

    export CP="$KAFKA/libs/*:$JEE/glassfish4/glassfish/modules/javax.json.jar"

    (for compilation only)

    export CLASSPATH=$MYSRC/sveta.jar:$JEE/glassfish4/glassfish/modules/javax.json.jar

    (for execution only)

create jar:

    cd $MYSRC

    javac -cp $CP *.java

    jar -cvf sveta.jar *.java


## create a small app that reads this data from kafka and prints it to stdout

### send topic items to stdout

    bin/kafka-run-class.sh MyCAT streams-file-input

### parse JSON (extract key,value)

    bin/kafka-run-class.sh MyJSON tamedia uid


## find a suitable data structure for counting and implement a simple counting mechanism, output the results to stdout 

### compute cardinality of values for a given JSON key

    bin/kafka-run-class.sh MyCARDH tamedia uid

    (to be fixed, doesn't give correct result, I am not conviced abot JSON parsing, 

    zcat ... | head -1000 | jq .uid -r | sort -u      result is 997 , my java code result is 936)  


# advanced solution

## benchmark

    todo

## Output to a new Kafka Topic instead of stdout

PipeDemo.java explains how to copy from one topic to another without filtering.

        KStreamBuilder builder = new KStreamBuilder();
        
        builder.stream("streams-file-input").to("streams-pipe-output");

WordCountDemo.java uses KTable to filter stream and to materialize in another stream (topic) 

        KTable<String, Long> counts = ...
        
        counts.to(Serdes.String(), Serdes.Long(), "streams-wordcount-output");
        
## try to measure performance and optimize

Some performance issues are visible, programs done so far are a little bit dumb.

This will bi taken into account and scalability is the right direction 

## write about how you could scale

Very short for now, I just start thinking about that:

Set up a multi broker cluster, as explained in http://kafka.apache.org/quickstart

Choose proper algorithm for cardinality 

http://highscalability.com/blog/2012/4/5/big-data-counting-how-to-count-a-billion-distinct-objects-us.html

(Yannik pointed to that)

HashSet method - maybe that is possible, I think about KTable to use instead HashSet, 

there is example WordCountDemo.java on github.com/apache/kafka. It is similar to database views :)

After filtering (to investigate), it is possible to materialize that "view", e.g. to send to another topic. 

Hyperloglog and Linear are right solution regarding scalability (partition work and than join), 

I'll try to find some simpliest possible existing algorithm for loglog and to implement that.  

## only now think about the edge cases, options and other things

todo

### Current issues

I use Virtual box instance of Oracle Linux 6.2 (1 cpu, 2GB ram) and it is quite old installation.

I did not have time to invent something better in this moment. 

Kafka fails to accept lots of input, so I tested with no more than 1000 lines from test file. 

Also there are some warrning messages when I ran Java from Kafka, but result is OK. 


