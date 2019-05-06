package io.exp.beampoc.model.PI.beam;

import io.exp.beampoc.model.PI.GregoryLeibniz_Term;
import io.exp.beampoc.model.PI.PI_Term;
import io.exp.beampoc.model.PI.PiInfiniteSeriesFactory;
import org.junit.Test;

import static org.junit.Assert.*;

public class BeamCalcTermTest {

    @Test
    public void of() {
        //BeamCalcTerm.of<PI_Term>("abc",null);
        BeamCalcPiTerm.of("abcd",null);

        BeamCalcTerm<PI_Term> p = new BeamCalcTerm<PI_Term>();
        p.JobKey="abcd";
        p.term = PiInfiniteSeriesFactory.createTerm("Nilakantha",1) ;

        //BeamCalcTerm<PI_Term>.create("abcd",null);

    }
}