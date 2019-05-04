package io.exp.beampoc.model.PI.generate;

import io.exp.beampoc.model.PI.PiInstruction;

import java.util.stream.Stream;


public class PiInstructionGenerator {
    final static int maxLimit=10;
    final static String[] SeriesNames={"Nilakantha","GregoryLeibniz"};


    public static Stream<PiInstruction> randomInstructionStream(){
        Stream<PiInstruction> stream = Stream.generate(() -> {
            PiInstruction p = new PiInstruction();

            int rn = (int)Math.floor( Math.random() * ( SeriesNames.length) );
            p.SeriesName = SeriesNames[rn];
            int rStep = (int)Math.floor(Math.random() * 5)+1;
            p.numOfSteps = 1000 * rStep;
            return p;
        }).limit(maxLimit);

        return stream;
    }



}
