package com.gizemgozde.loganalysis.loganalysisconsumer;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Properties;

@Component
public class KafkaStreamingService {

    public KafkaStreamingService() {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1500);

//        To get data produced before process started
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> source = builder.stream("demoteb");

        KStream<String, String> stats = source.groupByKey().aggregate(CityLogCountStatistics::new,
                (k, v, clusterstats) -> clusterstats.add(v),
                        TimeWindows.of(60000).advanceBy(10000),
                Serdes.serdeFrom(new MySerde(), new MySerde()), "data-store").toStream((key, value) -> key.key().toString() + " " +
                key.window().start()).mapValues((job) -> job.computeAvgTime().toString());

        stats.to(Serdes.String(), Serdes.String(),  "demotebout");

        KafkaStreams streams = new KafkaStreams(builder, props);

        System.out.println("STREAM YAPACAMMMM-------");
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

   /**
            * Do not use this serializer in production.
     */
    static class MySerde implements Serializer<CityLogCountStatistics>, Deserializer<CityLogCountStatistics> {

        @Override
        public byte[] serialize(String s, CityLogCountStatistics cStats){
            try(ByteArrayOutputStream b = new ByteArrayOutputStream()){
                try(ObjectOutputStream o = new ObjectOutputStream(b)){
                    o.writeObject(cStats);
                }
                return b.toByteArray();
            } catch (IOException e){
                e.printStackTrace();
                return null;
            }
        }

        @Override
        public CityLogCountStatistics deserialize(String topic, byte[] bytes) {
            try(ByteArrayInputStream b = new ByteArrayInputStream(bytes)){
                try(ObjectInputStream o = new ObjectInputStream(b)){
                    return (CityLogCountStatistics) o.readObject();
                }
            }catch (IOException | ClassNotFoundException e ){
                e.printStackTrace();
                return null;
            }
        }

        @Override
        public void configure(Map<String, ?> map, boolean b) {
        }

        @Override
        public void close() {

        }
    }
}
