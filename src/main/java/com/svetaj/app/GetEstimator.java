
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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.io.StringReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

/**
 * PRINTS TOPIC CONTENTS TO STDOUT AS SOON TOPIC RECEIVES NEW DATA
 * PRINTS CARDINALITY ESTIMATION CALCULATED AS BIT SUM FROM FIELD "est"
 *
 * USAGE: bin/kafka-run-class.sh GetEstimator topic_name
 */

public class GetEstimator {


    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                                 + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }

    private static class GetEstimatorProcessorSupplier implements ProcessorSupplier<String, String> {

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

                    try {
                        JSONParser parser = new JSONParser();
                        Object obj = parser.parse(new StringReader(line));
                        JSONObject jsonObject = (JSONObject) obj;
                        String est = (String) jsonObject.get("est");
                        long ts = (Long) jsonObject.get("ts");
                        long range = (Long) jsonObject.get("range");
                        long ec = (Long) jsonObject.get("ec");
                        byte[] map = hexStringToByteArray(est);
                        long c = 0;
                        for (byte b : map) {
                           c += Integer.bitCount(b & 0xFF);
                        }

	                JSONObject obj1 = new JSONObject();
                        long card = ec*8 - c;
	                obj1.put("est", card);
	                obj1.put("ts", ts);
	                obj1.put("range", range);
	                obj1.put("ec", ec);
                        System.out.println(obj1.toJSONString());
                    }
                    catch (IOException ex) {
                        ex.printStackTrace();
                    }
                    catch (ParseException ex) {
                        ex.printStackTrace();
                    }
                    catch (NullPointerException ex) {
                        ex.printStackTrace();
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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "get-est-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("Source", args[0]);

        builder.addProcessor("Process", new GetEstimatorProcessorSupplier(), "Source");

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();

        // usually the stream application would be running forever,
        // in this example we just let it run for some time and stop since the input data is finite.
        Thread.sleep(150000L);

        streams.close();
    }
}
