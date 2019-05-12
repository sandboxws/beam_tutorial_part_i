package io.exp.beampoc.model.PI.Model;

import java.io.Serializable;

@FunctionalInterface
public interface PI_FinalCalc extends Serializable {
    public double finalCalculation(double accumOfSeries);
}
