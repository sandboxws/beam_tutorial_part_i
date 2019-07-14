package io.exp.apachebeam.inmemory;

import io.exp.beampoc.stream.PI.Model.PiInstruction;
import io.exp.beampoc.stream.PI.generate.PiInstructionGenerator;
import io.exp.beampoc.stream.PI.workflow.BeamPiRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.stream.Stream;

public class BeamPiRun {

    public static void main(String[] args){
        Stream<PiInstruction> s = PiInstructionGenerator.randomInstructionStream(10,100,"Nilakantha");

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> pJson= BeamPiRunner.readInstruction2JsonPipeline(pipeline,s);
        PCollection<PiInstruction> pInst = BeamPiRunner.convertJSON2InstructionPipeline(pJson);
        PCollection<KV<String, Double>> dC=pInst.apply(new BeamPiRunner.CalculatePiWorkflow());
        dC.apply(ParDo.of(
                new DoFn<KV<String, Double>, Void>() {
                    @ProcessElement
                    public void processElement(@Element KV<String, Double> e){
                        System.out.println(e.getKey()+":"+e.getValue());
                    }
                }
        ));

        pipeline.run().waitUntilFinish();
    }
}
