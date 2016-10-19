
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

/**
 * PRINTS TOPIC CONTENTS TO STDOUT AS SOON TOPIC RECEIVES NEW DATA
 * based on WordCountProcessorDemo.java
 *     from package org.apache.kafka.streams.examples.wordcount
 * USAGE: bin/kafka-run-class.sh MyCAT topic_name
 */

public class MyCAT {

    private static class MyCATProcessorSupplier implements ProcessorSupplier<String, String> {

        @Override
        public Processor<String, String> get() {
            return new Processor<String, String>() {
                private ProcessorContext context;

                @Override
                @SuppressWarnings("unchecked")
                public void init(ProcessorContext context) {
                    this.context = context;
                    this.context.schedule(1000);
                }

                @Override
                public void process(String dummy, String line) {
                    System.out.println("DATA "+line);
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

        builder.addProcessor("Process", new MyCATProcessorSupplier(), "Source");

        KafkaStreams streams = new KafkaStreams(builder, props);
        System.out.println("-----------------------");
        System.out.println("TOPIC "+args[0]);
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
