package io.exp.apachebeam.kafka;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.apache.kafka.common.serialization.StringSerializer;

class Term implements Serializable {
    private int number;
    public Term(int n){
        this.number=n;
    }

    public Term (Term t){
        this.number=t.number;
    }
    public void multiply(int n){
        this.number*=n;
    }

    @Override
    public String toString() {
        return "I am "+number+".\n";
    }


}


public class RunBatchPipline {
    private static Logger LOGGER = LoggerFactory.getLogger(RunBatchPipline.class);

    static class ExtractWordsFn extends DoFn<String, String> {
        private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
        private final Distribution lineLenDist =
                Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            lineLenDist.update(element.length());
            if (element.trim().isEmpty()) {
                emptyLines.inc();
            }

            // Split the line into words.
            String[] words = element.split(",", -1);

            // Output each word encountered into the output PCollection.
            for (String word : words) {
                if (!word.isEmpty()) {
                    receiver.output(word);
                }
            }
        }
    }

    public static void main(String[] args){
        LOGGER.debug("Run the kafka connection");
        KafkaProducerValuesExample.insertStrings();
    }

     static class KafkaProducerValuesExample {
        static final String[] WORDS_ARRAY = new String[] {
                "hi there", "hi", "hi sue bob",
                "hi sue", "", "bob hi"};
        static String BOOTSTRAPSERVER="localhost:9092";
        static String TestTopic="test2";

        static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

        public static void insertStrings() {
            PipelineOptions options = PipelineOptionsFactory.create();
            Pipeline p = Pipeline.create(options);

            PCollection<String> input = p.apply(Create.of(WORDS).withCoder(StringUtf8Coder.of()));
            input.apply(KafkaIO.<Void, String>write()
                    .withBootstrapServers(BOOTSTRAPSERVER)
                    .withTopic(TestTopic)
                    .withValueSerializer(StringSerializer.class)
                    .values()
            );

            p.run().waitUntilFinish();
        }
    }
}

