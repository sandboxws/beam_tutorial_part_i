package io.exp.apachebeam.text;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import io.exp.apachebeam.Model.ExecutePipelineOptions;
import io.exp.beampoc.stream.PI.Model.PiInstruction;
import io.exp.beampoc.stream.PI.workflow.BeamPiRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class BeamPiRun {
    private final static Logger LOGGER = LoggerFactory.getLogger(BeamPiRun.class);


    public static PiInstruction convertStr2Instruction(String line){
        Gson gson = new Gson();
        PiInstruction inst =null;
        try{
            inst = gson.fromJson(line,PiInstruction.class);
        }catch(JsonSyntaxException je){
            inst=null;
        }
        return inst;
    }

    public static void main(String[] args){
        ExecutePipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(ExecutePipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        PCollection<PiInstruction> pInst = pipeline.apply("Read from text data", TextIO.read().from(options.getInputFile()) )
                .apply(ParDo.of(new DoFn<String, PiInstruction>() {
                    @ProcessElement
                    public void processElement(@Element String element, OutputReceiver<PiInstruction> out){
                        Optional<PiInstruction> i = Optional.ofNullable(convertStr2Instruction(element));
                        i.ifPresent(inst -> {
                            out.output(inst);
                        });

                    }
                }));



        //PCollection<KV<String, Double>> dC=BeamPiRunner.generatePiWorkflow(pInst);
        PCollection<KV<String, Double>> dC=pInst.apply(new BeamPiRunner.CalculatePiWorkflow());

        dC.apply(ParDo.of(
                new DoFn<KV<String, Double>, String>() {
                    @ProcessElement
                    public void processElement(@Element KV<String, Double> e,OutputReceiver<String> out){

                        String str = e.getKey()+":"+e.getValue();
                        out.output(str);
                    }
                }
        )).apply(TextIO.write().to(options.getOutput()).withSuffix(".out"));

        pipeline.run().waitUntilFinish();
    }
}
