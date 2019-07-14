package io.exp.kafka;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class KafkaConsumerRunnerTest {

    //@Test
    public void testconsume() {
        KafkaConsumerRunner k = KafkaConsumerRunner.of("192.168.99.104:9094","test1");
        String topic = "pi";
        k.consume(topic);

    }

    @Test
    public void getConsumerMap() {
        Map<String, Object> m = KafkaConsumerRunner.getConsumerMap("localhost",9092,"test1");
        assertEquals(m.size(),6);
        assertEquals(m.get("enable.auto.commit"),"true");
    }
}