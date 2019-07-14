package io.exp.beampoc.stream.PI.Model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.HashMap;
import java.util.Map;


public class PiInfiniteSeriesFactory {
    final static Logger logger= LoggerFactory.getLogger(PiInfiniteSeriesFactory.class);
    final public static String packageName= "io.exp.beampoc.stream.PI.Model.";

    public volatile static Map<String, Class> classMap = new HashMap<String, Class>();

    public final static PI_Term createTerm(String SeriesName, int term){
        PI_Term t = null;
        Class c=null;
        try {
            c = PiInfiniteSeriesFactory.getClass(SeriesName);
            t=(PI_Term)c.newInstance();
            assert(t!=null);
            t.setTerm(term);

            return t;
        }catch(Exception e){
            logger.error(e.getMessage());
            return null;
        }

    }
    public final static PI_FinalCalc getFinalCalc(String SeriesName,int totalTerm){
        PI_FinalCalc finalCalc=null;
        try {
            Class c = PiInfiniteSeriesFactory.getClass(SeriesName);
            PI_Term t =((PI_Term)c.newInstance());
            t.setTotalTerm(totalTerm);
            finalCalc=t.getFinalCalculation();
            return finalCalc;
        }catch(Exception e){
            logger.error(e.getMessage());
            finalCalc=null;
            return null;
        }

    }

    public final static Class getClass(String SeriesName) throws ClassNotFoundException {
        String _className = packageName+SeriesName+"_Term";
        Class c =null;


        if(classMap.get(SeriesName) == null){
            synchronized (classMap) {
                if (classMap.get(SeriesName) == null) {
                    //synchronized (classMap) {
                    //if (classMap.get(SeriesName) == null) {
                    Class cc = Class.forName(_className);
                    classMap.put(SeriesName, cc);
                    return cc;
                    //}
                    //}
                }
            }
        }

        c=classMap.get(SeriesName);
        assert(c!=null);
        return c;
    }
}
