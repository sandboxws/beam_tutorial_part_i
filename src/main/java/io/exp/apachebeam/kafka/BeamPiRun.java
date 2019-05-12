package io.exp.apachebeam.kafka;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import io.exp.apachebeam.Model.ExecutePipelineOptions;
import io.exp.beampoc.stream.PI.Model.PiInstruction;
import io.exp.beampoc.stream.PI.workflow.BeamPiRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class BeamPiRun {
    private final static Logger LOGGER = LoggerFactory.getLogger(io.exp.apachebeam.text.BeamPiRun.class);



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
        PCollection<String> pStr = pipeline.apply(KafkaIO.<String, String>read()
                .withBootstrapServers(options.getBootStrapServer())
                .withTopic(options.getTopic())
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)

                .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object)"earliest"))

                // We're writing to a file, which does not support unbounded data sources. This line makes it bounded to
                // the first 5 records.
                // In reality, we would likely be writing to a data source that supports unbounded data, such as BigQuery.
                .withMaxNumRecords(5)

                .withoutMetadata() // PCollection<KV<Long, String>>
        ).apply(Values.<String>create());

        PCollection<PiInstruction> pInst=pStr.apply(ParDo.of(new DoFn<String, PiInstruction>() {
            @ProcessElement
            public void processElement(@Element String element, OutputReceiver<PiInstruction> out){
                Optional<PiInstruction> i = Optional.ofNullable(convertStr2Instruction(element));
                i.ifPresent(inst -> {
                    out.output(inst);
                });

            }
        }));;
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
