package io.exp.beampoc.model.PI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class PiInfiniteSeriesFactory {
    final static Logger logger= LoggerFactory.getLogger(PiInfiniteSeriesFactory.class);
    final public static String packageName= "io.exp.beampoc.model.PI.";

    public final static Map<String, Class> classMap = new HashMap<String, Class>();

    public final static PI_Term createTerm(String SeriesName, int term){
        PI_Term t = null;
        try {
            Class c = PiInfiniteSeriesFactory.getClass(SeriesName);
            t=(PI_Term)c.newInstance();
            t.setTerm(term);
        }catch(Exception e){
            logger.error(e.getMessage());
            t=null;
        }
        return t;
    }
    public final static PI_FinalCalc getFinalCalc(String SeriesName){
        PI_FinalCalc finalCalc=null;
        try {
            Class c = PiInfiniteSeriesFactory.getClass(SeriesName);
            finalCalc=((PI_Term)c.newInstance()).getFinalCalculation();

        }catch(Exception e){
            logger.error(e.getMessage());
            finalCalc=null;
        }
        return finalCalc;
    }

    public final static Class getClass(String SeriesName) throws ClassNotFoundException {
        String _className = packageName+SeriesName+"_Term";
        Class c =classMap.get(SeriesName);
        if(c == null){
            synchronized ((classMap)){
                if(classMap.get(SeriesName)==null){
                    c=Class.forName(_className);
                    classMap.put(SeriesName,c);
                }
            }
        }

        return c;
    }
}
