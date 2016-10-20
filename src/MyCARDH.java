
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.Locale;
import java.util.Properties;
import java.util.HashSet;

import javax.json.Json;
import javax.json.stream.JsonParser;
import java.io.StringReader;

/**
 * PRINTS CARDINALITY OF JSON VALUES ASSOCIATED WITG GIVEN JSON KEY
 * BASED ON TOPIC CONTENTS - DUMB VERSION - JUST FOR EXISTING CONTENTS
 * AND NOT TAKING INTO ACCOUNT TIMESTAMP AND INTERVAL 
 * based on HashSet method and WordCountProcessorDemo.java
 *     from package org.apache.kafka.streams.examples.wordcount
 * and "Reading JSON Data Using a Parser" from Java EE Tutorial 
 * USAGE: bin/kafka-run-class.sh MyCARDH topic_name key_name
 *        where key_name is JSON key
 */

public class MyCARDH {
    
    private static String key;
    private static HashSet<String> hs;
    private static int count;   // cardinality

    private static class MyCARDHProcessorSupplier implements ProcessorSupplier<String, String> {

        @Override
        public Processor<String, String> get() {
            return new Processor<String, String>() {
                private ProcessorContext context;

                @Override
                @SuppressWarnings("unchecked")
                public void init(ProcessorContext context) {
                    this.context = context;
                    this.context.schedule(1000);
                    hs = new HashSet<String>();
                    count = 0;
                }

                @Override
                public void process(String dummy, String line) {
                    JsonParser parser = Json.createParser(new StringReader(line));
                    boolean keyx = false;
                    while (parser.hasNext()) {
                       JsonParser.Event event = parser.next();
                       switch(event) {
                          case START_ARRAY:
                          case END_ARRAY:
                          case START_OBJECT:
                          case END_OBJECT:
                          case VALUE_FALSE:
                          case VALUE_NULL:
                          case VALUE_TRUE:
                             break;
                          case KEY_NAME:
                             if (parser.getString().equals(key)) keyx = true;
                             break;
                          case VALUE_NUMBER:
                          case VALUE_STRING:
                             if (keyx) { 
                                 String val = parser.getString();
                                 if (!hs.contains(val)) {
                                    count++;
                                    hs.add(val);
                                    System.out.println("CARDINALITY ="+count+" NEW VAL="+(val+"XXXXXXXXXX").substring(0,10));
                                 }
                                 keyx = false;
                              }
                              break;
                       }
                    }
                }

                @Override
                public void punctuate(long timestamp) {
                }

                @Override
                public void close() {
                }
            };
        }
    }

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-cat-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("Source", args[0]);

        key = args[1];
        builder.addProcessor("Process", new MyCARDHProcessorSupplier(), "Source");

        KafkaStreams streams = new KafkaStreams(builder, props);
        System.out.println("-----------------------");
        System.out.println("TOPIC "+args[0]);
        System.out.println("JSON KEY "+args[1]);
        System.out.println("-----------------------");
        System.out.println("BEGINNING OF TOPIC DATA");
        System.out.println("-----------------------");
        streams.start();

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
        Thread.sleep(150000L);
        System.out.println("-----------------------");
        System.out.println("END OF TOPIC DATA");
        System.out.println("-----------------------");

        streams.close();
    }
}
