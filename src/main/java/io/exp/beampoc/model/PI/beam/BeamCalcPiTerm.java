package io.exp.beampoc.model.PI.beam;

import io.exp.beampoc.model.PI.PI_Term;

import java.io.Serializable;

public class BeamCalcPiTerm  implements Serializable {
    public String JobKey;
    public PI_Term term;

    public static  BeamCalcPiTerm of (String jobkey, PI_Term term){
        BeamCalcPiTerm o= new BeamCalcPiTerm();
        o.JobKey=jobkey;
        o.term=term;
        return o;
    }
}

