package io.exp.beampoc.stream.PI.workflow;

import io.exp.beampoc.stream.PI.Model.PI_FinalCalc;
import io.exp.beampoc.stream.PI.Model.PI_Term;
import io.exp.beampoc.stream.PI.Model.PiInfiniteSeriesFactory;
import io.exp.beampoc.stream.PI.Model.PiInstruction;

import io.exp.beampoc.stream.PI.beam.BeamCalcTerm;
import org.apache.beam.sdk.Pipeline;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.*;

import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import org.apache.beam.sdk.values.TupleTag;
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

    public static PCollection<KV<String, Double>> generatePiWorkflow(PCollection<PiInstruction> pIn){
        PCollection<KV<String, Double>> piOut=null;


        PCollection<BeamCalcTerm<PI_Term> > calcOut =pIn.apply("BeamCalcTerm",
                ParDo.of(
                        new DoFn<PiInstruction, BeamCalcTerm<PI_Term>>() {
                            @ProcessElement
                            public void processElement(@Element PiInstruction c, OutputReceiver< BeamCalcTerm<PI_Term> > out) {
                                for (int i=0;i<c.numOfSteps;i++) {
                                    PI_Term t = PiInfiniteSeriesFactory.createTerm(c.SeriesName,i);
                                    BeamCalcTerm<PI_Term> p = BeamCalcTerm.of(c.id,t);
                                    assert(p.term!=null);
                                    out.output(p);
                                }
                                LOGGER.debug("Setup:"+c.id+" Seriesname:"+c.SeriesName+" steps:"+c.numOfSteps);
                            }
                        }
                )
        );
        final PCollection< KV<String, PI_FinalCalc> > pFinalStep = pIn.apply(
                ParDo.of(
                        new DoFn<PiInstruction, KV<String, PI_FinalCalc> >(){
                            @ProcessElement
                            public void processElement(@Element PiInstruction c, OutputReceiver < KV <String, PI_FinalCalc> > out ){
                                PI_FinalCalc finalCalc=PiInfiniteSeriesFactory.getFinalCalc(c.SeriesName,c.numOfSteps);
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

                                        //BeamCalcTerm<Double> dd = new BeamCalcTerm<Double>();
                                        //dd.JobKey = t.JobKey;
                                        //dd.term = d;
                                        BeamCalcTerm<Double> dd = BeamCalcTerm.of(t.JobKey,d);
                                        out.output(dd);
                                    }
                                    // LOGGER.debug("Grp:"+dd.JobKey+"Term:"+(ts).term.getTerm() +":"+dd.term);
                                });
                            }
                        }
                ));

        /*
        PCollection<KV<String, Double> > SumOut = pOut.apply(

                "Map_term", MapElements.via(new SimpleFunction< BeamCalcTerm<Double>, KV<String, Double>>() {
                    public KV<String, Double> apply(BeamCalcTerm<Double> element) {
                        return KV.of(element.JobKey, (Double)element.term);
                    }
                })
        ).apply(perKey());


        //CoGroup by Key
        TupleTag<Double> SeriesTag = new TupleTag<>();
        TupleTag<PI_FinalCalc> finalizeTag = new TupleTag<>();
        PCollection< KV<String, CoGbkResult> > MergeResult = KeyedPCollectionTuple.of(SeriesTag, SumOut)
                .and(finalizeTag, pFinalStep)
                .apply(CoGroupByKey.create());


        //Final Pi values
        PCollection < KV<String, Double> > finalPi= MergeResult.apply(
                ParDo.of(
                        new DoFn<KV<String, CoGbkResult>, KV<String, Double>>() {
                            @ProcessElement
                            public void processElement(@Element KV<String, CoGbkResult> r, OutputReceiver< KV<String, Double> > out){
                                Double sumValueItr = r.getValue().getOnly(SeriesTag);
                                PI_FinalCalc finalCalc = r.getValue().getOnly(finalizeTag);
                                String k = r.getKey();
                                double value = finalCalc.finalCalculation(sumValueItr);
                                out.output( KV.of (k,value) );
                            }
                        }
                )
        );*/

        piOut=null;
        return piOut;
    }
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

    public static class CalculatePiWorkflow
            extends PTransform<PCollection<PiInstruction>, PCollection<KV<String,Double>> > {



        @Override
        public PCollection< KV<String, Double> > expand(PCollection<PiInstruction> pIn) {


            PCollection<BeamCalcTerm<PI_Term> > calcOut =pIn.apply("BeamCalcTerm",
                    ParDo.of(
                            new DoFn<PiInstruction, BeamCalcTerm<PI_Term>>() {
                                @ProcessElement
                                public void processElement(@Element PiInstruction c, OutputReceiver< BeamCalcTerm<PI_Term> > out) {
                                    for (int i=0;i<c.numOfSteps;i++) {
                                        PI_Term t = PiInfiniteSeriesFactory.createTerm(c.SeriesName,i);

                                        //BeamCalcTerm<PI_Term> p = new BeamCalcTerm<PI_Term>();
                                        //p.JobKey=c.id;
                                        //p.term = t;

                                        BeamCalcTerm<PI_Term> p = BeamCalcTerm.of(c.id,t);
                                        assert(p.term!=null);
                                        out.output(p);
                                    }
                                    LOGGER.debug("Setup:"+c.id+" Seriesname:"+c.SeriesName+" steps:"+c.numOfSteps);
                                }
                            }
                    )
            );
            final PCollection< KV<String, PI_FinalCalc> > pFinalStep = pIn.apply(
                    ParDo.of(
                            new DoFn<PiInstruction, KV<String, PI_FinalCalc> >(){
                                @ProcessElement
                                public void processElement(@Element PiInstruction c, OutputReceiver < KV <String, PI_FinalCalc> > out ){
                                    PI_FinalCalc finalCalc=PiInfiniteSeriesFactory.getFinalCalc(c.SeriesName,c.numOfSteps);
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

                                    //BeamCalcTerm<Double> dd = new BeamCalcTerm<Double>();
                                    //dd.JobKey = t.JobKey;
                                    //dd.term = d;
                                    BeamCalcTerm<Double> dd = BeamCalcTerm.of(t.JobKey,d);
                                    out.output(dd);
                                }
                               // LOGGER.debug("Grp:"+dd.JobKey+"Term:"+(ts).term.getTerm() +":"+dd.term);
                            });
                        }
                    }
            ));

            PCollection<KV<String, Double> > SumOut = pOut.apply(

                    "Map_term", MapElements.via(new SimpleFunction< BeamCalcTerm<Double>, KV<String, Double>>() {
                        public KV<String, Double> apply(BeamCalcTerm<Double> element) {
                            return KV.of(element.JobKey, (Double)element.term);
                        }
                    })
            ).apply(perKey());


            //CoGroup by Key
            TupleTag<Double> SeriesTag = new TupleTag<>();
            TupleTag<PI_FinalCalc> finalizeTag = new TupleTag<>();
            PCollection< KV<String, CoGbkResult> > MergeResult = KeyedPCollectionTuple.of(SeriesTag, SumOut)
                    .and(finalizeTag, pFinalStep)
                    .apply(CoGroupByKey.create());

            //Final Pi values
            PCollection < KV<String, Double> > finalPi= MergeResult.apply(
                    ParDo.of(
                            new DoFn<KV<String, CoGbkResult>, KV<String, Double>>() {
                                @ProcessElement
                                public void processElement(@Element KV<String, CoGbkResult> r, OutputReceiver< KV<String, Double> > out){
                                    Double sumValueItr = r.getValue().getOnly(SeriesTag);
                                    PI_FinalCalc finalCalc = r.getValue().getOnly(finalizeTag);
                                    String k = r.getKey();
                                    double value = finalCalc.finalCalculation(sumValueItr);
                                    out.output( KV.of (k,value) );
                                }
                            }
                    )
            );

//            PCollection< Double > dOut = finalPi.apply(
//                    ParDo.of(
//                            new DoFn<  KV<String, Double> , Double>() {
//                                @ProcessElement
//                                public void processElement(@Element KV<String, Double> dd , OutputReceiver<Double> out){
//                                    out.output(dd.getValue());
//                                    LOGGER.debug("Grp:"+dd.getKey()+" value:"+dd.getValue());
//                                }
//                            }
//                    )
//            );
//
//            PCollection<Double> outputDbl = dOut;
            return finalPi;
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
