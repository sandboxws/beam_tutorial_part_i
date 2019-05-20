package io.exp.kafka.Main;

import io.exp.beampoc.stream.PI.Model.PIInstructionFactory;
import io.exp.beampoc.stream.PI.Model.PiInstruction;
import io.exp.kafka.KafkaPublisherRunner;

public class KafkaPublisherMain {
    public static void main(String [] args){
        String bootstrapserver = args[0];
        int series = Integer.parseInt(args[1]);
        int numOfRequest = Integer.parseInt(args[2]);
        int numStep = Integer.parseInt(args[3]);

        KafkaPublisherRunner k = KafkaPublisherRunner.of(bootstrapserver);
        String topic = "pi";

        for(int ii=0;ii<numOfRequest;ii++) {
            PiInstruction pi = PIInstructionFactory.createInstruction(PIInstructionFactory.SupportedSeries[series], numStep);

            k.publish(topic, pi.id, pi.toString());
        }
    }
}
