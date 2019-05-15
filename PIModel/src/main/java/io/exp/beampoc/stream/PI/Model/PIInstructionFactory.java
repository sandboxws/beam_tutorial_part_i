package io.exp.beampoc.stream.PI.Model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PIInstructionFactory {
    final static Logger logger= LoggerFactory.getLogger(PIInstructionFactory.class);
    final public static String packageName= "io.exp.beampoc.stream.PI.Model.";
    final public static String[] SupportedSeries = {"MonteCarlo","Nilakantha","GregoryLeibniz"};


    public final static PiInstruction createInstruction(String SeriesName, int step)  {
        String _className = packageName+SeriesName+"_Term";
        try {
            Class cc = Class.forName(_className);
            if(cc==null){
                throw new IllegalArgumentException("Cannot support "+SeriesName);
            }
        }catch(ClassNotFoundException ce){
            throw new IllegalArgumentException("Cannot support "+SeriesName);
        }


        if(step<=0){
            throw new IllegalArgumentException("Step should be larger than 1");
        }
        PiInstruction pi = new PiInstruction();
        pi.numOfSteps=step;
        pi.SeriesName=SeriesName;
        return pi;
    }

}
