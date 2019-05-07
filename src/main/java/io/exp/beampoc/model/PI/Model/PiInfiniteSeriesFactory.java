package io.exp.beampoc.model.PI.Model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PiInfiniteSeriesFactory {
    final static Logger logger= LoggerFactory.getLogger(PiInfiniteSeriesFactory.class);
    final public static String packageName= "io.exp.beampoc.model.PI.Model.";

    //public final static Map<String, Class> classMap = new HashMap<String, Class>();

    public final static PI_Term createTerm(String SeriesName, int term){
        PI_Term t = null;
        Class c=null;
        try {
            c = PiInfiniteSeriesFactory.getClass(SeriesName);
            t=(PI_Term)c.newInstance();
            assert(t!=null);
            t.setTerm(term);

        }catch(Exception e){
            logger.error(e.getMessage());

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
        Class c =null;
        /*
        Cannot be used under Apache Beam
        c=classMap.get(SeriesName);
        if(c == null){
            synchronized ((classMap)){
                if(classMap.get(SeriesName)==null){
                    c=Class.forName(_className);
                    classMap.put(SeriesName,c);
                }
            }
        }*/

        c=Class.forName(_className);
        return c;
    }
}
