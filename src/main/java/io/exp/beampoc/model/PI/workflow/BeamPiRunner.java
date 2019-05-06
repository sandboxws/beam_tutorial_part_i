package io.exp.beampoc.model.PI.workflow;

import io.exp.beampoc.model.PI.*;
import org.apache.beam.sdk.Pipeline;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.Collectors;
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
