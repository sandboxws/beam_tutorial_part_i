package io.exp.beampoc.model.PI.workflow;

import io.exp.beampoc.model.PI.Nilakantha_Term;
import io.exp.beampoc.model.PI.PI_Term;
import io.exp.beampoc.model.PI.PiInfiniteSeriesFactory;
import io.exp.beampoc.model.PI.PiInstruction;
import org.apache.beam.sdk.Pipeline;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

    //Step 3
    public static PCollection<PI_Term> generatePiTermfromPiInstruction(PCollection<PiInstruction> pIn){
        PCollection<PI_Term> pOut = pIn.apply(ParDo.of(
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
        return pOut;
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


    static class CalculatePiWorkflow2
            extends PTransform<PCollection<PiInstruction>, PCollection<Double>> {
        @Override
        public PCollection<Double> expand(PCollection<PiInstruction> pIn) {

            PCollection<PI_Term> pOut = pIn.apply(ParDo.of(
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

            PCollection<Double> dOut=pOut.apply(ParDo.of(
                    new DoFn<PI_Term, Double>() {
                        @ProcessElement
                        public void processElement(@Element PI_Term t, OutputReceiver<Double> out){
                            //LOGGER.debug("Term:"+(t).getTerm() +":"+t.calculateTerm());
                            Double d = t.calculateTerm();
                            out.output(d);
                        }
                    }
            )).apply(Combine.globally(new AccumPiTermCalculation()));
            return dOut;
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
