package io.exp.beampoc.model.PI.generate;

import io.exp.beampoc.model.PI.PiInstruction;
import org.junit.Test;

import java.util.Iterator;
import java.util.stream.Stream;

public class PiInstructionGeneratorTest {

    @Test
    public void randomInstructionStream() {
        Stream<PiInstruction> s = PiInstructionGenerator.randomInstructionStream();

        Iterator<PiInstruction> itr=s.iterator();
        while(itr.hasNext()){
            PiInstruction p = itr.next();
            System.out.println(p.toString());
        }

    }
}