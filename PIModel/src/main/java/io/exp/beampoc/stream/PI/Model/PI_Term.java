package io.exp.beampoc.stream.PI.Model;

import java.io.Serializable;

public interface PI_Term extends Serializable {
    public int getTerm();
    public void setTerm(int term);
    public double calculateTerm();
    public void setTotalTerm(int total);
    public int getTotalTerm();
    //public double finalCalculation(double accumOfSeries);

    public PI_FinalCalc getFinalCalculation();
}
