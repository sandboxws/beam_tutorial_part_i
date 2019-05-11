package io.exp.beampoc.stream.PI.beam;

import java.io.Serializable;

public class BeamCalcTerm<T> implements Serializable {
    public String JobKey;
    public T term;

    private BeamCalcTerm(){}

    public static <T> BeamCalcTerm<T> create (String jobkey,T term){
        return new BeamCalcTerm<T>();

    }

    public static <T> BeamCalcTerm<T> of (String jobkey, T term){
        BeamCalcTerm<T> o= new BeamCalcTerm<T>();
        o.JobKey=jobkey;
        o.term=term;
        return o;
    }
}
