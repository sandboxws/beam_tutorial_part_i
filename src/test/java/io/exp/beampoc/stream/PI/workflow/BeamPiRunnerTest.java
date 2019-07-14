package io.exp.beampoc.stream.PI.workflow;

import io.exp.beampoc.stream.PI.Model.PiInstruction;
import io.exp.beampoc.stream.PI.generate.PiInstructionGenerator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.number.IsCloseTo;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.*;

class COmpositeProcessWords
        extends PTransform<PCollection<String>, PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollection<String> lines) {
        String TOKENIZER_PATTERN = "[^\\p{L}]+";
        // Convert lines of text into individual words.
        PCollection<String> words = lines.apply(ParDo.of(
                new DoFn<String, String>() {
                    @ProcessElement

                    public void processElement(ProcessContext c) {
                            for (String word : c.element().split(TOKENIZER_PATTERN)) {
                                if (!word.isEmpty()) {
                                    c.output(word);
                                }
                            }
                        }
                }
        ));
        return words;
    }
}

class SingleProcessWords extends DoFn<String, String>{
    String TOKENIZER_PATTERN = "[^\\p{L}]+";
    @ProcessElement
    public void processElement(ProcessContext c) {
        for (String word : c.element().split(TOKENIZER_PATTERN)) {
            if (!word.isEmpty()) {
                c.output(word);
            }
        }
    }
}
class Json2PiInstruction extends DoFn<String, PiInstruction> {
    @ProcessElement
    public void processElement(@Element String c, OutputReceiver<PiInstruction> out) {
        PiInstruction pi = PiInstruction.fromJson(c);
        out.output(pi);
    }
}
class PiInstruction2Json extends DoFn< PiInstruction,String> {
    @ProcessElement
    public void processElement(@Element PiInstruction c, OutputReceiver<String> out) {
        out.output(c.toString());
    }
}

class NilakanthaCheckPiResult implements SerializableFunction<Iterable<KV< String, Double> >, Void>{
    @Override
    public Void apply(Iterable<KV<String, Double>> input) {

        long cnt=0;
        for (KV< String, Double> v : input) {
            cnt++;
            double pi = v.getValue();

            double diff = Math.abs(pi-Math.PI);
            assertThat(diff, new IsCloseTo(0,1e-3));
        }

        return  null;
    }

}
public class BeamPiRunnerTest {

    //@Test
    public void test(){
        String[] WORDS_ARRAY = new String[] {
                "hi there", "hi", "hi sue bob",
                "hi sue", "", "bob hi"};

        List<String> WORDS = Arrays.asList(WORDS_ARRAY);

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);
        PCollection<String> input = p.apply(Create.of(WORDS)).setCoder(StringUtf8Coder.of());
        PCollection<String> output =input.apply( ParDo.of(new SingleProcessWords()));//.apply(TextIO.write().to("./multiplyresults").withSuffix(".txt"));;
        output.apply(TextIO.write().to("./multiplyresults").withSuffix(".txt"));;
        p.run().waitUntilFinish();
    }

//  Run as anonymous class of ParDo, not working!!!
//    class ProcessWords
//            extends PTransform<PCollection<String>, PCollection<String>> {
//        final static String TOKENIZER_PATTERN = "[^\\p{L}]+";
//        @Override
//        public PCollection<String> expand(PCollection<String> lines) {
//
//            // Convert lines of text into individual words.
//            /*
//            PCollection<String> words = lines.apply(ParDo.of(
//                    new DoFn<String, String>() {
//                        @ProcessElement
//                        public void processElement(ProcessContext c) {
//                            for (String word : c.element().split(TOKENIZER_PATTERN)) {
//                                if (!word.isEmpty()) {
//                                    c.output(word);
//                                }
//                            }
//                        }
//                    }
//            ));*/
//            // Convert lines of text into individual words.
//            PCollection<String> words = lines.apply(ParDo.of(
//                    new DoFn<String, String>() {
//                        @ProcessElement
//                        public void processElement(@Element String e , OutputReceiver<String> out) {
//                            String processedString = e+" procssed";
//                            out.output(processedString);
//                        }
//                    }
//            ));
//
//
//            return words;
//        }
//    }

    @Test
    public void constructInstructionPipeline() {

        Stream<PiInstruction> s = PiInstructionGenerator.randomInstructionStream(10,0);

        PipelineOptions options = PipelineOptionsFactory.create();

        // Create the Pipeline object with the options we defined above.

        Pipeline pipeline = Pipeline.create(options);
/*
        CoderRegistry cr = pipeline.getCoderRegistry();
        cr.registerCoderForClass(String.class, StringUtf8Coder.of());
*/

        PCollection<String> pJson=BeamPiRunner.readInstruction2JsonPipeline(pipeline,s);
        PCollection<PiInstruction> pInst = BeamPiRunner.convertJSON2InstructionPipeline(pJson);
/*
        BeamPiRunner.convertInstruction2JsonPipeline(pInst)
                .apply(TextIO.write().to("./instresults").withSuffix(".txt"));
                */
       /*
        pInst.apply(ParDo.of(
                //Not workable as anoymous class
                new PiInstruction2Json()
        )).apply(TextIO.write().to("./instresults").withSuffix(".txt"));;*/
        pipeline.run().waitUntilFinish();

    }

    @Test
    public void generatePiTermfromPiInstruction() {
        Stream<PiInstruction> s = PiInstructionGenerator.randomInstructionStream(20,100,"Nilakantha");

        PipelineOptions options = PipelineOptionsFactory.create();

        // Create the Pipeline object with the options we defined above.

        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> pJson=BeamPiRunner.readInstruction2JsonPipeline(pipeline,s);
        PCollection<PiInstruction> pInst = BeamPiRunner.convertJSON2InstructionPipeline(pJson);

        PCollection<KV<String, Double>> dC=pInst.apply(new BeamPiRunner.CalculatePiWorkflow());


        PAssert.that(dC).satisfies(
                new NilakanthaCheckPiResult()

//                IN Unit test, anonymous class of SerializableFunction not working
//                new SerializableFunction<Iterable<Double>, Void>(){
//            @Override
//            public Void apply(Iterable<Double> input) {
//                double value=0.0;
//                long cnt=0;
//                for (Double v : input) {
//                    cnt++;
//                    value = v;
//                }
//
//                assert(cnt==1);
//                double pi = 3+value;
//
//                return  null;
//            }
//        }
        );

        pipeline.run().waitUntilFinish();
    }
}