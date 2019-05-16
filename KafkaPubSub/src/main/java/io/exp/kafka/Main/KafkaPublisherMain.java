package io.exp.kafka.Main;

import io.exp.beampoc.stream.PI.Model.PIInstructionFactory;
import io.exp.beampoc.stream.PI.Model.PiInstruction;
import io.exp.kafka.KafkaPublisherRunner;

public class KafkaPublisherMain {
    public static void main(String [] args){
        int series = Integer.parseInt(args[0]);
        int numOfRequest = Integer.parseInt(args[1]);
        int numStep = Integer.parseInt(args[2]);

        KafkaPublisherRunner k = KafkaPublisherRunner.of("localhost",9092);
        String topic = "pi";

        for(int ii=0;ii<numOfRequest;ii++) {
            PiInstruction pi = PIInstructionFactory.createInstruction(PIInstructionFactory.SupportedSeries[series], numStep);

            k.publish(topic, pi.id, pi.toString());
        }
    }
}
