import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class GestionTemperature {

    public static class MonitoringEvent {

        public String type;
        public int value;

        public MonitoringEvent(String event, int value){
            this.type = event;
            this.value = value;
        }

        @Override
        public String toString() {
            return type + " : " + value;
        }

    }


    public static void main(String[] args) throws Exception {
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.enableCheckpointing(5000); // checkpoint every 5000 msecs

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "ece1.adaltas.com:9093,ece2.adaltas.com:9093,ece3.adaltas.com:9093");
        properties.setProperty("AdrienM", "testid");


        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<>("test", new SimpleStringSchema(), properties);
        myConsumer.setStartFromEarliest();     // start from the earliest record possible
        //myConsumer.setStartFromLatest();       // start from the latest record
        //myConsumer.setStartFromTimestamp(...); // start from specified epoch timestamp (milliseconds)
        //myConsumer.setStartFromGroupOffsets(); // the default behaviour

        // get input data
        DataStream<String> kafka = env.addSource(myConsumer);

    /*
      Stream transformations
    */
        // < Entrée, Sortie >
        DataStream<WordWithCount> counts = kafka
                .flatMap(new LineSplitter())
                .keyBy("word")
                .timeWindow(Time.seconds(10))
                .reduce(new CountReduce());
    }
}


public class KafkaWordCount {

    public static class WordWithCount {

        public String word;
        public int count;
        public WordWithCount() {}

        public WordWithCount(String word, int count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.enableCheckpointing(5000); // checkpoint every 5000 msecs

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "ece1.adaltas.com:9093,ece2.adaltas.com:9093,ece3.adaltas.com:9093");
        properties.setProperty("AdrienM", "testid");


        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<>("monitoring", new SimpleStringSchema(), properties);
        //myConsumer.setStartFromEarliest();     // start from the earliest record possible
        myConsumer.setStartFromLatest();       // start from the latest record
        //myConsumer.setStartFromTimestamp(...); // start from specified epoch timestamp (milliseconds)
        //myConsumer.setStartFromGroupOffsets(); // the default behaviour

        // get input data
        DataStream<String> kafka = env.addSource(myConsumer);


        DataStream<WordWithCount> counts = kafka
                .flatMap(new LineSplitter());
               /* .timeWindow(Time.seconds(10)) */
            /*    .reduce(new PatternReduce()); */


        counts.print().setParallelism(1);

        // execute program
        env.execute("Kafka WordCount");
    }


    //Cela va séparer les valeurs dans les lignes du fichier
    public static class LineSplitter implements FlatMapFunction<String, WordWithCount> {

        @Override
        public void flatMap(String s, Collector<WordWithCount> collector) throws Exception {
            String[] tokens = s.split(",");

            for(String t:tokens){
                collector.collect(new WordWithCount(t, 1));
            }
        }
    }

    public static final class PatternReduce implements ReduceFunction<WordWithCount> {

        @Override
        public WordWithCount reduce(WordWithCount t1, WordWithCount t2) throws Exception {

        }
    }
}
