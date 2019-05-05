package io.exp.beampoc.model.PI;

import java.io.Serializable;

public interface PI_Term extends Serializable {
    public int getTerm();
    public void setTerm(int term);
    public double calculateTerm();
    public double finalCalculation(double accumOfSeries);
}
