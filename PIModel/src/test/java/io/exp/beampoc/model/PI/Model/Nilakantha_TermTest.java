package io.exp.beampoc.model.PI.Model;


import io.exp.beampoc.model.PI.Model.Nilakantha_Term;
import io.exp.beampoc.model.PI.Model.PI_Term;
import org.hamcrest.number.IsCloseTo;
import org.junit.Test;


import static org.junit.Assert.*;

public class Nilakantha_TermTest {

    class Accum{
        double a=0;
    }
    @Test
    public void calculateTerm() {


        double d=0;
        for (int i=0;i<10000;i++){
            PI_Term t = new Nilakantha_Term(i);
            d+=( t).calculateTerm();
        }

        double pi = new Nilakantha_Term(0).getFinalCalculation().finalCalculation(d);
        double diff = Math.abs(pi-Math.PI);
        System.out.println(pi);
        assertThat(diff, new IsCloseTo(0,1e-11));
    }
}