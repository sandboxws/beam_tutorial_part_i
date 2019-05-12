package io.exp.beampoc.model.PI.Model;

import org.junit.Test;

import static org.junit.Assert.*;

public class PIInstructionFactoryTest {

    @Test
    public void shouldThrowExceptionIfSeriesNotFound() throws Exception{

        try {
            PiInstruction p = PIInstructionFactory.createInstruction("abcd", 100);
            fail("Should throw exception of invalid seriesname");
        }catch(IllegalArgumentException ie){
            assert (ie instanceof  IllegalArgumentException);
        }
    }

    @Test
    public void shouldThrowExceptionIfSteplessthan1() throws Exception{

        try {
            PiInstruction p = PIInstructionFactory.createInstruction("Nilakantha", 0);
            fail("Should throw exception of invalid seriesname");
        }catch(IllegalArgumentException ie){
            assert (ie instanceof  IllegalArgumentException);
        }
    }
    @Test
    public void shouldGenerateInstruction() throws Exception{


            PiInstruction p = PIInstructionFactory.createInstruction("Nilakantha", 100);

        assertEquals(p.numOfSteps,100);

    }
}