package io.exp.kafka;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaPublisherRunner {
    final static Logger logger= LoggerFactory.getLogger(KafkaPublisherRunner.class);
    Properties props = null;

    private KafkaPublisherRunner(){
        props = new Properties();
    }

    public static Properties setupProperty(String brokerHostname, int brokerPort){
        Properties p = new Properties();
        String bootstrapserver = brokerHostname+":"+brokerPort;
        p.put("bootstrap.servers", bootstrapserver);
        p.put("acks", "all");
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return p;
    }
    public static ImmutableMap<String,String> getPublisherMap(String brokerHostName, int brokerPort){
        Properties p = setupProperty(brokerHostName,brokerPort);
        return com.google.common.collect.Maps.fromProperties(p);
    }

    public static KafkaPublisherRunner of(String hostname, int brokerPort){
        KafkaPublisherRunner p = new KafkaPublisherRunner();
        p.props = setupProperty(hostname,brokerPort);
        return p;
    }

    public void publish(String topic,String key, String value) throws KafkaException{
        Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());

        try{
            producer.send(new ProducerRecord<>(topic, key, value));
        }catch(KafkaException ke){
            logger.error(ke.getMessage());
            throw ke;
        }
        finally{
            producer.close();
        }
        /*
        Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());

        producer.initTransactions();

        try {
            producer.beginTransaction();
            for (int i = 0; i < 100; i++)
                producer.send(new ProducerRecord<>("my-topic", Integer.toString(i), Integer.toString(i)));
            producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            producer.close();
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            producer.abortTransaction();
        }
        producer.close();
        */

    }
}
