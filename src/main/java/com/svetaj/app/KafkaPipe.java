
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

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.io.StringReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import com.clearspring.analytics.stream.cardinality.LinearCounting;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;

/**
 * PRINTS CARDINALITY OF JSON VALUES ASSOCIATED WITH GIVEN JSON KEY
 * BASED ON TOPIC CONTENTS - DUMB VERSION - JUST FOR EXISTING CONTENTS
 * AND NOT TAKING INTO ACCOUNT TIMESTAMP AND INTERVAL 
 *
 * USAGE: bin/kafka-run-class.sh KafkaPipe kafka_topic_name json_key_name  [HASHSET] [LOGLOG] [LINEAR]
 */

public class KafkaPipe {
    
    private static String json_key;
    private static String json_ts;
    private static HashSet<String> hs;
    private static HyperLogLog hl;
    private static LinearCounting lc;
    private static boolean hs_flag;
    private static boolean hl_flag;
    private static boolean lc_flag;
    private static boolean debug;
 
    private static void initCounting() {
         if (hs_flag) hs = new HashSet<String>();
         if (hl_flag) hl = new HyperLogLog(10);
         if (lc_flag) lc = new LinearCounting(150000);
         debug = true;
    }

    private static void processCounting(String line) {
        try {
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(new StringReader(line));
            JSONObject jsonObject = (JSONObject) obj;
            String val = (String) jsonObject.get(json_key);
            boolean added = false;
            if (hs_flag && hs.add(val)) {
                 if (debug) System.out.print("HASHSET="+hs.size()+" ");
                 added = true;
            }
            if (hl_flag && hl.offer(val)) {
                 if (debug) System.out.print("LOGLOG="+hl.cardinality()+" ");
                 added = true;
            }
            if (lc_flag && lc.offer(val)) {
                 if (debug) System.out.print("LINEAR="+lc.cardinality()+" ");
                 added = true;
            }
            if (debug && added) System.out.println(" NEW VAL="+(val+"XXXXXXXXXX").substring(0,10));
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

    private static void punctuateCounting(long timestamp) {}

    private static void closeCounting() {}

    private static KafkaStreams initKafkaStream(String src_str) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-cat-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("Source", src_str);

        builder.addProcessor("Process", new KafkaPipeProcessorSupplier(), "Source");

        return new KafkaStreams(builder, props);
    }


    private static class KafkaPipeProcessorSupplier implements ProcessorSupplier<String, String> {

        @Override
        public Processor<String, String> get() {
            return new Processor<String, String>() {
                private ProcessorContext context;

                @Override
                @SuppressWarnings("unchecked")
                public void init(ProcessorContext context) {
                    this.context = context;
                    this.context.schedule(1000);
                    initCounting();
                }

                @Override
                public void process(String dummy, String line) {
                    processCounting(line);
                }

                @Override
                public void punctuate(long timestamp) {
                    punctuateCounting(timestamp);
                }

                @Override
                public void close() {
                    closeCounting();
                }
            };
        }
    }

    public static void main(String[] args) throws Exception {
        boolean std_in = false;
        KafkaStreams kafka_stream = null;
        BufferedReader br = null;

        if(args.length < 3) { 
            System.out.println("USAGE: bin/kafka-run-class.sh KafkaPipe kafka_topic_name|stdin ");
            System.out.println("json_key_name [HASHSET] [LINEAR] [LOGLOG]\n"); 
            System.exit(1);
        }
        String source_stream = args[0];
        std_in = source_stream.equals("stdin");
        json_key = args[1];
        for (int i=2; i < args.length; i++) {
           if (i >= 5) break;
           if (args[i].equals("HASHSET")) hs_flag = true;
           if (args[i].equals("LOGLOG"))  hl_flag = true;
           if (args[i].equals("LINEAR"))  lc_flag = true;
        }

        if (!std_in) kafka_stream = initKafkaStream(source_stream);

        debug = true;
        if (debug) {
            System.out.println("-----------------------");
            System.out.println("TOPIC "+source_stream);
            System.out.println("JSON KEY "+json_key);
            if (hs_flag) System.out.println("HASHSET");
            if (hl_flag) System.out.println("LOGLOG");
            if (lc_flag) System.out.println("LINEAR");
            System.out.println("-----------------------");
            System.out.println("BEGINNING OF TOPIC DATA");
            System.out.println("-----------------------");
        }
        if (std_in) {
            try {
                 initCounting();
                 br = new BufferedReader(new InputStreamReader(System.in));
                 String line = "";
                 while(true){
                     line = br.readLine();
                     if (line == null) break;
                     processCounting(line);
                 }
            } catch (Exception e) {
            }
        }
        else
            kafka_stream.start();

        // avoid running forever
        Thread.sleep(150000L);
        if (debug) {
            System.out.println("-----------------------");
            System.out.println("END OF TOPIC DATA");
            System.out.println("-----------------------");
        }

        if (!std_in) kafka_stream.close();
    }
}
