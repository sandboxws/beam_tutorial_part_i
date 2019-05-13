package io.exp.kafka;

import org.junit.Test;

import static org.junit.Assert.*;

public class KafkaConsumerRunnerTest {

    //@Test
    public void testconsume() {
        KafkaConsumerRunner k = KafkaConsumerRunner.of("localhost",9092,"test1");
        String topic = "pi";
        k.consume(topic);

    }
}