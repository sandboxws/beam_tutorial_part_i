package io.exp.beampoc.model.PI.generate;

import io.exp.beampoc.model.PI.PiInstruction;
import org.junit.Test;

import java.util.Iterator;
import java.util.stream.Stream;

public class PiInstructionGeneratorTest {

    @Test
    public void randomInstructionStream() {
        final int cnt_Total=10;
        Stream<PiInstruction> s = PiInstructionGenerator.randomInstructionStream( cnt_Total,0);

        Iterator<PiInstruction> itr=s.iterator();
        int cnt=0;
        while(itr.hasNext()){
            PiInstruction p = itr.next();
            System.out.println(p.toString());
            cnt++;
        }
        assert (cnt==cnt_Total);
    }

    @Test
    public void returnIllegalArugmentExceptionWhenSeriesNameNotFound() {
        final int cnt_Total=10;

        try {
            Stream<PiInstruction> s = PiInstructionGenerator.randomInstructionStream( cnt_Total,0,"ABcd");

            Iterator<PiInstruction> itr=s.iterator();
            int cnt = 0;
            while (itr.hasNext()) {
                PiInstruction p = itr.next();
                System.out.println(p.toString());
                cnt++;
            }
            throw new RuntimeException("Should throw exception");
        }catch(IllegalArgumentException ie){

        }

    }
    @Test
    public void returnSeriesName() {
        final int cnt_Total=10;

            Stream<PiInstruction> s = PiInstructionGenerator.randomInstructionStream( cnt_Total,0,"Nilakantha");

            Iterator<PiInstruction> itr=s.iterator();
            int cnt = 0;
            while (itr.hasNext()) {
                PiInstruction p = itr.next();
                System.out.println(p.toString());
                cnt++;
                assert("Nilakantha".equals(p.SeriesName));
                assert(p.numOfSteps>=1000);
            }
            assert(cnt==cnt_Total);
    }
    @Test
    public void return5SeriesName() {
        final int cnt_Total=1;
        final int maxStep=5;

        Stream<PiInstruction> s = PiInstructionGenerator.randomInstructionStream( cnt_Total,maxStep,"Nilakantha");

        Iterator<PiInstruction> itr=s.iterator();
        int cnt = 0;
        while (itr.hasNext()) {
            PiInstruction p = itr.next();
            System.out.println(p.toString());
            cnt++;
            assert("Nilakantha".equals(p.SeriesName));
            assert (maxStep == p.numOfSteps);
        }


    }

}