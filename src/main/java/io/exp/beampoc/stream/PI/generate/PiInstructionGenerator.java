package io.exp.beampoc.stream.PI.generate;

import io.exp.beampoc.stream.PI.Model.PiInstruction;

import java.util.Arrays;
import java.util.stream.Stream;


public class PiInstructionGenerator {
    //final static int maxLimit=10;
    final static String[] SeriesNames={"Nilakantha","GregoryLeibniz"};


    public static Stream<PiInstruction> randomInstructionStream(int maxItems, int maxStep,String... name){
        long forceSeries=0;
        String[] forceSeriesName={""};
        if(name.length>0){
            forceSeriesName[0]=name[0];
            forceSeries=Arrays.stream(SeriesNames).filter(x->x.equals(forceSeriesName[0])).count();

            if(forceSeries ==0){
                throw new IllegalArgumentException("SeriesName is not found:"+forceSeriesName[0]);
            }
        }
        Stream<PiInstruction> stream = Stream.generate(() -> {
            PiInstruction p = new PiInstruction();
            if(name.length==0) {
                int rn = (int) Math.floor(Math.random() * (SeriesNames.length));
                p.SeriesName = SeriesNames[rn];
            }else{
                p.SeriesName=forceSeriesName[0];
            }
            int rStep = 0;
            rStep = (maxStep==0)? ((int) Math.floor(Math.random() * 5) + 1)*1000:maxStep;

            p.numOfSteps =rStep;
            return p;
        }).limit(maxItems);

        return stream;
    }



}
