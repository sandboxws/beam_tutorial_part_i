package io.exp.beampoc.model.PI.workflow;

import io.exp.beampoc.model.PI.*;
import io.exp.beampoc.model.PI.beam.BeamCalcPiTerm;

import io.exp.beampoc.model.PI.beam.BeamCalcTerm;
import org.apache.beam.sdk.Pipeline;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.*;

import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import org.slf4j.LoggerFactory;


import java.util.LinkedList;
import java.util.List;

import java.util.Optional;

import java.util.stream.Stream;





public class BeamPiRunner {

    private static org.slf4j.Logger LOGGER = LoggerFactory.getLogger(BeamPiRunner.class);

    //step1
    public static PCollection<String> readInstruction2JsonPipeline(Pipeline p, Stream<PiInstruction> stream) {

        List<String> lm = new LinkedList<String>();
        stream.forEach(e -> {
            lm.add(e.toString());
        });

        PCollection<String> pPiInstruction = p.apply(Create.of(lm)).setCoder(StringUtf8Coder.of());

        return pPiInstruction;
    }

    //step2
    public static PCollection<PiInstruction> convertJSON2InstructionPipeline(PCollection<String> p) {
        PCollection<PiInstruction> pIn = p.apply(ParDo.of(

                new DoFn<String, PiInstruction>() {
                    @ProcessElement
                    public void processElement(@Element String c, OutputReceiver<PiInstruction> out) {
                        PiInstruction pi = PiInstruction.fromJson(c);
                        out.output(pi);
                    }
                }
        ));

        return pIn;
    }

    //Not needed
//    //Step 3
//    public static PCollection<PI_Term> generatePiTermfromPiInstruction(PCollection<PiInstruction> pIn){
//        PCollection<PI_Term> pOut = pIn.apply(ParDo.of(
//                new DoFn<PiInstruction, PI_Term>() {
//                    @ProcessElement
//                    public void processElement(@Element PiInstruction c, OutputReceiver<PI_Term> out) {
//                        for (int i=0;i<c.numOfSteps;i++){
//                            PI_Term t = PiInfiniteSeriesFactory.createTerm(c.SeriesName,i);
//                            out.output(t);
//                        }
//                    }
//                }
//        ));
//
//        return pOut;
//    }




    /**
     * A PTransform that converts a PCollection containing lines of text into a PCollection of
     * formatted word counts.
     *
     * <p>Concept #3: This is a custom composite transform that bundles three transforms
     *
     *
     * as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
     * modular testing, and an improved monitoring experience.
     */


    static class CalculatePiWorkflow
            extends PTransform<PCollection<PiInstruction>, PCollection<Double>> {
        @Override
        public PCollection<Double> expand(PCollection<PiInstruction> pIn) {

            PCollection<PI_Term> pOut = pIn.apply("PiTerms",ParDo.of(
                    new DoFn<PiInstruction, PI_Term>() {
                        @ProcessElement
                        public void processElement(@Element PiInstruction c, OutputReceiver<PI_Term> out) {
                            for (int i=0;i<c.numOfSteps;i++){
                                PI_Term t = PiInfiniteSeriesFactory.createTerm(c.SeriesName,i);
                                out.output(t);

                            }
                        }
                    }
            ));

            /*
            final PCollection<KV<String,PI_FinalCalc>> pFinal = pIn.apply(ParDo.of(
                    new DoFn<PiInstruction, PI_FinalCalc>() {
                        @ProcessElement
                        public void processElement(@Element PiInstruction c, OutputReceiver<PI_FinalCalc> out){
                            PI_FinalCalc finalCalc=PiInfiniteSeriesFactory.getFinalCalc(c.SeriesName);
                            out.output(finalCalc);
                        }
                    }
            ));*/

            PCollection<Double> dOut=pOut.apply("Calculate_PiTerms",ParDo.of(
                    new DoFn<PI_Term, Double>() {
                        @ProcessElement
                        public void processElement(@Element PI_Term t, OutputReceiver<Double> out){

                            Optional<PI_Term> tt = Optional.ofNullable(t);
                            tt.ifPresent(term->{
                                Double d = t.calculateTerm();
                                out.output(d);
                                LOGGER.debug("Term:"+(t).getTerm() +":"+t.calculateTerm());
                            });
                        }
                    }
            )).apply("Agg_PiTerms",Combine.globally(new AccumPiTermCalculation()));


            PCollection<Double> fOut =dOut.apply("Finalize",ParDo.of(
                    new DoFn<Double, Double>() {
                        @ProcessElement
                        public void processElement(@Element Double d , OutputReceiver<Double> out, ProcessContext c){

                        }
                    }
            )
            );

            PCollection<Double> outputDbl = dOut;
            return outputDbl;
        }
    }


    static class CalculatePiWorkflow2
            extends PTransform<PCollection<PiInstruction>, PCollection<Double>> {

        public static PTransform<PCollection<KV<String, java.lang.Double>>, PCollection<KV<String, Double>>> perKey() {
            return Combine.perKey(new SerializableFunction<Iterable<Double>, Double>() {
                @Override
                public Double apply(Iterable<Double> input) {
                    double accum=0.0;

                    for (Double d : input){
                        accum+= d;
                    }
                    return accum;
                }
            });
        }

        @Override
        public PCollection< Double > expand(PCollection<PiInstruction> pIn) {

//            PCollection<BeamCalcPiTerm> calcOut = pIn.apply("BeamCalcTerm",
//                    ParDo.of(
//                            new DoFn<PiInstruction, BeamCalcPiTerm>() {
//                                @ProcessElement
//                                public void processElement(@Element PiInstruction c, OutputReceiver< BeamCalcPiTerm > out) {
//                                    for (int i=0;i<c.numOfSteps;i++){
//                                        PI_Term t = PiInfiniteSeriesFactory.createTerm(c.SeriesName,i);
//                                        out.output(BeamCalcPiTerm.of(c.id,t));
//
//                                    }
//                                }
//                            }
//
//                    )
//            );
            PCollection<BeamCalcTerm<PI_Term> > calcOut =pIn.apply("BeamCalcTerm",
                    ParDo.of(
                            new DoFn<PiInstruction, BeamCalcTerm<PI_Term>>() {
                                @ProcessElement
                                public void processElement(@Element PiInstruction c, OutputReceiver< BeamCalcTerm<PI_Term> > out) {
                                    for (int i=0;i<c.numOfSteps;i++) {
                                        PI_Term t = PiInfiniteSeriesFactory.createTerm(c.SeriesName,i);
                                        BeamCalcTerm<PI_Term> p = new BeamCalcTerm<PI_Term>();
                                        p.JobKey=c.id;
                                        p.term = t;
                                        assert(p.term!=null);
                                        out.output(p);
                                    }
                                    LOGGER.debug("Setup:"+c.id+"Seriesname:"+c.SeriesName+":"+c.numOfSteps+"steps");
                                }
                            }
                    )
            );
            final PCollection< KV<String, PI_FinalCalc> > pFinal = pIn.apply(
                    ParDo.of(
                            new DoFn<PiInstruction, KV<String, PI_FinalCalc> >(){
                                @ProcessElement
                                public void processElement(@Element PiInstruction c, OutputReceiver < KV <String, PI_FinalCalc> > out ){
                                    PI_FinalCalc finalCalc=PiInfiniteSeriesFactory.getFinalCalc(c.SeriesName);
                                    out.output( KV.of(c.id, finalCalc) );
                                }
                            }
                    )

            );

            PCollection< BeamCalcTerm<Double> > pOut=calcOut.apply("Calculate_PiTerms",
                    ParDo.of(
                    new DoFn< BeamCalcTerm<PI_Term>, BeamCalcTerm<Double> >() {
                        @ProcessElement
                        public void processElement(@Element BeamCalcTerm<PI_Term> t, OutputReceiver< BeamCalcTerm<Double> > out){

                            Optional< BeamCalcTerm<PI_Term> > tt = Optional.ofNullable(t);
                            tt.ifPresent(ts->{
                                Optional<PI_Term> term = Optional.ofNullable(ts.term);
                                if(term.isPresent()) {
                                    double d = ts.term.calculateTerm();
                                    BeamCalcTerm<Double> dd = new BeamCalcTerm<Double>();
                                    dd.JobKey = t.JobKey;
                                    dd.term = d;
                                    out.output(dd);
                                }
                               // LOGGER.debug("Grp:"+dd.JobKey+"Term:"+(ts).term.getTerm() +":"+dd.term);
                            });
                        }
                    }
            ));

            PCollection<KV<String, Double> > mOut = pOut.apply(

                    "Map_term", MapElements.via(new SimpleFunction< BeamCalcTerm<Double>, KV<String, Double>>() {
                        public KV<String, Double> apply(BeamCalcTerm<Double> element) {
                            return KV.of(element.JobKey, (Double)element.term);
                        }
                    })
            ).apply(perKey());




            PCollection< Double > dOut = mOut.apply(
                    ParDo.of(
                            new DoFn<  KV<String, Double> , Double>() {
                                @ProcessElement
                                public void processElement(@Element KV<String, Double> dd , OutputReceiver<Double> out){
                                    out.output(dd.getValue());
                                    LOGGER.debug("Grp:"+dd.getKey()+" value:"+dd.getValue());
                                }
                            }
                    )
            );

            /*
            PCollection< KV<String, PI_Term> > pOut = pIn.apply("PiTerms",
                    MapElements.via(new SimpleFunction<PiInstruction, KV<String, PI_Term>>() {
                        public KV<String, PI_Term> apply(PiInstruction element) {

                            for (int i=0;i<c.numOfSteps;i++){
                                PI_Term t = PiInfiniteSeriesFactory.createTerm(c.SeriesName,i);
                                out.output(t);

                            }
                            return KV.of(element, (Void)null);
                        }
                    })

            );*/




            PCollection<Double> outputDbl = dOut;
            return outputDbl;
        }
    }
/*
    class Json2PiInstruction extends DoFn<String, PiInstruction> {
        @ProcessElement
        public void processElement(@Element String c, OutputReceiver<PiInstruction> out) {
            PiInstruction pi = PiInstruction.fromJson(c);
            out.output(pi);
        }
    }*/

    //testing function
    public static PCollection<String> convertInstruction2JsonPipeline(PCollection<PiInstruction> p) {
        PCollection<String> pStr = p.apply(ParDo.of(
                new DoFn<PiInstruction, String>() {
                    @ProcessElement
                    public void processElement(@Element PiInstruction c, OutputReceiver<String> out) {
                        out.output(c.toString());
                    }
                }
        ));
        return pStr;
    }

}
