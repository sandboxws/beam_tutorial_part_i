package io.exp.beampoc.stream.PI.Model;

import java.io.Serializable;

@FunctionalInterface
public interface PI_FinalCalc extends Serializable {
    public double finalCalculation(double accumOfSeries);
}
