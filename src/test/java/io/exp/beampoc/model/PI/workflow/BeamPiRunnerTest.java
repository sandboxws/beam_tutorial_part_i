package io.exp.beampoc.model.PI.workflow;

import io.exp.beampoc.model.PI.PiInstruction;
import io.exp.beampoc.model.PI.generate.PiInstructionGenerator;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.*;

class ProcessWords
        extends PTransform<PCollection<String>, PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollection<String> lines) {
        String TOKENIZER_PATTERN = "[^\\p{L}]+";
        // Convert lines of text into individual words.
        PCollection<String> words = lines.apply(ParDo.of(
                new DoFn<String, String>() {
                    @ProcessElement
                    /*
                    public void processElement(@Element String e , OutputReceiver<String> out) {
                        String processedString = e+" procssed";
                        out.output(processedString);
                    }*/
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
public class BeamPiRunnerTest {

    @Test
    public void test(){
        String[] WORDS_ARRAY = new String[] {
                "hi there", "hi", "hi sue bob",
                "hi sue", "", "bob hi"};

        List<String> WORDS = Arrays.asList(WORDS_ARRAY);

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);
        PCollection<String> input = p.apply(Create.of(WORDS)).setCoder(StringUtf8Coder.of());
        PCollection<String> output =input.apply(new ProcessWords());//.apply(TextIO.write().to("./multiplyresults").withSuffix(".txt"));;
        output.apply(TextIO.write().to("./multiplyresults").withSuffix(".txt"));;
        p.run().waitUntilFinish();
    }

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

    //@Test
    public void constructInstructionPipeline() {

        Stream<PiInstruction> s = PiInstructionGenerator.randomInstructionStream();

        PipelineOptions options = PipelineOptionsFactory.create();

        // Create the Pipeline object with the options we defined above.

        Pipeline pipeline = Pipeline.create(options);

        CoderRegistry cr = pipeline.getCoderRegistry();
        cr.registerCoderForClass(String.class, StringUtf8Coder.of());


        PCollection<String> pJson=BeamPiRunner.readInstruction2JsonPipeline(pipeline,s);
        //PCollection<PiInstruction> pInst = BeamPiRunner.convertJSON2InstructionPipeline(pJson);

        pJson.apply(ParDo.of(
                new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(@Element String c, OutputReceiver<String> out) {
                        out.output("Hello");

                    }
                }
        )).apply(TextIO.write().to("./multiplyresults").withSuffix(".txt"));;

        pipeline.run().waitUntilFinish();

    }
}