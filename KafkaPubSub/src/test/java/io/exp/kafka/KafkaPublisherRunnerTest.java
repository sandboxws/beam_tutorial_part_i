package io.exp.kafka;

import com.google.common.collect.ImmutableMap;

import io.exp.beampoc.stream.PI.Model.PIInstructionFactory;
import io.exp.beampoc.stream.PI.Model.PiInstruction;
import org.junit.Test;

import static org.junit.Assert.*;

public class KafkaPublisherRunnerTest {

    @Test
    public void publicToNewTopic()throws Exception {
        KafkaPublisherRunner k = KafkaPublisherRunner.of("localhost",9092);
        k.publish("abcd","123","<abcd2></abcd2>");
    }

    @Test
    public void publishMsgToTopic()throws Exception {
        KafkaPublisherRunner k = KafkaPublisherRunner.of("localhost",9092);
        k.publish("abcd","123","<abcd></abcd>");
    }

    @Test
    public void publishPiInstruction() throws Exception{
        KafkaPublisherRunner k = KafkaPublisherRunner.of("localhost",9092);
        String topic = "pi";

        PiInstruction pi = PIInstructionFactory.createInstruction(PIInstructionFactory.SupportedSeries[0],5000);

        k.publish(topic,pi.id,pi.toString());

    }
    @Test
    public void getPublisherMap() {
        ImmutableMap<String, String> m = KafkaPublisherRunner.getPublisherMap("localhost",9092);
        assertEquals(m.size(),4);
    }
}