package io.exp.kafka;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

public class KafkaConsumerRunner {
    final static Logger logger= LoggerFactory.getLogger(KafkaConsumerRunner.class);
    Properties props = null;

    boolean running=true;
    private KafkaConsumerRunner(){
        props = new Properties();
    }

    public static Properties setupProperty(String brokerHostname, int brokerPort,String consumerGrp){
        Properties p = new Properties();
        String bootstrapserver = brokerHostname+":"+brokerPort;
        p.put("bootstrap.servers", bootstrapserver);
        p.setProperty("group.id", consumerGrp);
        p.setProperty("enable.auto.commit", "true");
        p.setProperty("auto.commit.interval.ms", "1000");
        p.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return p;
    }

    public static KafkaConsumerRunner of(String brokerHostname, int brokerPort,String consumerGrp){
        KafkaConsumerRunner p = new KafkaConsumerRunner();
        p.props = setupProperty(brokerHostname,brokerPort,consumerGrp);
        return p;
    }
    public static Map<String,Object> getConsumerMap(String brokerHostName, int brokerPort,String consumerGrp){
        Properties p = setupProperty(brokerHostName,brokerPort,consumerGrp);

        ImmutableMap<String,String> v = com.google.common.collect.Maps.fromProperties(p);
        Map<String, Object> m = new TreeMap<String, Object>();
        v.forEach( (key,value)->{
            m.put(key,(Object)value);
        });
        return m;
    }



    public void consume(String topic) {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList((topic)));
        while (running) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }

    }

    public void stopConsume(){
            this.running=false;
    }


}
