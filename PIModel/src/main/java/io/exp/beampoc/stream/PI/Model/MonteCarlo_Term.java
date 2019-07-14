package io.exp.beampoc.stream.PI.Model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MonteCarlo_Term implements PI_Term{

    Logger LOGGER = LoggerFactory.getLogger(MonteCarlo_Term.class);
    int term;
    int totalTerm;
    final static int TOTALSIM=1000000;//Integer.MAX_VALUE;
    @Override
    public int getTerm() {
        return term;
    }

    @Override
    public void setTerm(int term) {
        this.term=term;
    }

    @Override
    public double calculateTerm() {
        int accum=0;
        for( int i=0 ;i<TOTALSIM;i++){
            double x = Math.random();
            double y= Math.random();
            double v= Math.sqrt(x*x + y*y);
            if(v<=1.0) {
                accum++;
            }

        }
        return (double)accum/(double)TOTALSIM;
    }

    @Override
    public void setTotalTerm(int total) {
        this.totalTerm=total;
    }

    @Override
    public int getTotalTerm() {
        return this.totalTerm;
    }

    @Override
    public PI_FinalCalc getFinalCalculation() {

        return  (accumOfSeries -> (  accumOfSeries/this.totalTerm * 4));
    }
}
