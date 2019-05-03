package io.exp.beampoc.model;

public class GregoryLeibniz_Term  implements PI_Term{
    int term;
    boolean isOddTerm=false;

    GregoryLeibniz_Term(){

    }
    GregoryLeibniz_Term(int t){
        this();
        this.setTerm(t);
    }

    public int getTerm() {
        return term;
    }

    @Override
    public void setTerm(int term) {
        if(term<0){
            throw new IllegalArgumentException("Term should be larger than 0");
        }
        this.term = term;
        isOddTerm=(term%2!=0);
    }

    @Override
    public double calculateTerm() {
        double y = 2.0*term+1.0;
        return isOddTerm?(-1/y):(1/y);
    }

    @Override
    public double finalCalculation(double accumOfSeries) {
        return accumOfSeries*4;
    }
}
