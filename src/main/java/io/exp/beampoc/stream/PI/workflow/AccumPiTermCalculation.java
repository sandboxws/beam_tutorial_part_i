package io.exp.beampoc.stream.PI.workflow;

import org.apache.beam.sdk.transforms.SerializableFunction;

public class AccumPiTermCalculation implements SerializableFunction<Iterable<Double>, Double> {
    @Override
    public Double apply(Iterable<Double> input) {

        double accum=0.0;

        for (Double d : input){
            accum+=d;
        }
        return accum;
    }
}
