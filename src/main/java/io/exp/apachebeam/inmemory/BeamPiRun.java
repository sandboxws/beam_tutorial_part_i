package io.exp.apachebeam.inmemory;

import io.exp.beampoc.model.PI.Model.PiInstruction;
import io.exp.beampoc.model.PI.generate.PiInstructionGenerator;
import io.exp.beampoc.model.PI.workflow.BeamPiRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.stream.Stream;

public class BeamPiRun {

    public static void main(String[] args){
        Stream<PiInstruction> s = PiInstructionGenerator.randomInstructionStream(10,100,"Nilakantha");

        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> pJson= BeamPiRunner.readInstruction2JsonPipeline(pipeline,s);
        PCollection<PiInstruction> pInst = BeamPiRunner.convertJSON2InstructionPipeline(pJson);
        PCollection<Double> dC=pInst.apply(new BeamPiRunner.CalculatePiWorkflow());
        dC.apply(ParDo.of(
                new DoFn<Double, Void>() {
                    @ProcessElement
                    public void processElement(@Element Double e){
                        System.out.println(e.doubleValue());
                    }

                }
        ));

        pipeline.run().waitUntilFinish();
    }
}
