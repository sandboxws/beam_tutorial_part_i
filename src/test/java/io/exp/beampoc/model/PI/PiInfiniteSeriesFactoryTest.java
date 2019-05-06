package io.exp.beampoc.model.PI;

import io.exp.beampoc.model.PI.Nilakantha_Term;
import io.exp.beampoc.model.PI.PI_Term;
import io.exp.beampoc.model.PI.PiInfiniteSeriesFactory;
import org.hamcrest.number.IsCloseTo;
import org.junit.Test;

import static org.junit.Assert.*;

public class PiInfiniteSeriesFactoryTest {

    @Test
    public void createTerm() {

        String seriesName="Nilakantha";
        double d=0;
        for (int i=0;i<10000;i++){
            PI_Term t = PiInfiniteSeriesFactory.createTerm(seriesName,i);
            d+=( t).calculateTerm();
        }
        double pi =  PiInfiniteSeriesFactory.getFinalCalc(seriesName).finalCalculation(d);
        double diff = Math.abs(pi-Math.PI);
        System.out.println(pi);
        assertThat(diff, new IsCloseTo(0,1e-11));
    }
}