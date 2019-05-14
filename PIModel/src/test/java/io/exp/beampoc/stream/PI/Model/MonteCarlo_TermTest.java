package io.exp.beampoc.stream.PI.Model;

import org.hamcrest.number.IsCloseTo;
import org.junit.Test;

import static org.junit.Assert.*;

public class MonteCarlo_TermTest {

    String seriesName="MonteCarlo";

    @Test
    public void calculateTerm() {

        //totally, 10B times of simulation to obtain accuracy up to 4 digits
        //10 min of run
        final int totalTerm=10000;
        double d=0;
        long start = System.currentTimeMillis();
        for (int i=0;i<totalTerm;i++){
            PI_Term t = PiInfiniteSeriesFactory.createTerm(seriesName,i);
            d+=( t).calculateTerm();
        }

        double pi =  PiInfiniteSeriesFactory.getFinalCalc(seriesName,totalTerm).finalCalculation(d);
        double diff = Math.abs(pi-Math.PI);
        long end = System.currentTimeMillis();
        System.out.println(pi);
        float sec = (end - start) / 1000F; System.out.println(sec + " seconds");
        assertThat(diff, new IsCloseTo(0,1e-4));
    }
}