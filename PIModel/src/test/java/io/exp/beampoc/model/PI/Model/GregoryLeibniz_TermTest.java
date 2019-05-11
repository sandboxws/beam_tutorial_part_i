package io.exp.beampoc.model.PI.Model;

import io.exp.beampoc.model.PI.Model.GregoryLeibniz_Term;
import io.exp.beampoc.model.PI.Model.PI_Term;
import org.hamcrest.number.IsCloseTo;
import org.junit.Test;

import static org.junit.Assert.*;

public class GregoryLeibniz_TermTest {

    @Test
    public void calculateTerm() {



        double d=0;
        for (int i=0;i<10000;i++){
            PI_Term t = new GregoryLeibniz_Term(i);
            d+=( t).calculateTerm();
        }

        double pi = (new GregoryLeibniz_Term(0)).getFinalCalculation().finalCalculation(d);
        double diff = Math.abs(pi-Math.PI);
        System.out.println(pi);
        assertThat(diff, new IsCloseTo(0,1e-3));
    }
}