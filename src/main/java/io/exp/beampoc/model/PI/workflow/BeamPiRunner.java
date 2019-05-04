package io.exp.beampoc.model.PI.workflow;

import io.exp.beampoc.model.PI.PiInstruction;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BeamPiRunner {


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

        return null;
    }
}
