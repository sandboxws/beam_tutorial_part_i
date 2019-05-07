package io.exp.apachebeam.kafka.healthcheck;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import com.google.common.collect.ImmutableMap;
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
        //KafkaProducerValuesExample.insertStrings();
        KafkaProducerValuesExample.readStrings();
    }
    static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

     static class KafkaProducerValuesExample {
        static final String[] WORDS_ARRAY = new String[] {
                "hi there", "hi", "hi sue bob",
                "hi sue", "", "bob hi"};
        static String BOOTSTRAPSERVER="localhost:9092";
        static String TestTopic="test";

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

        public static void readStrings(){
            PipelineOptions options = PipelineOptionsFactory.create();

            // Create the Pipeline object with the options we defined above.

            Pipeline p = Pipeline.create(options);
            p.apply(KafkaIO.<Long, String>read()
                    .withBootstrapServers(BOOTSTRAPSERVER)
                    .withTopic(TestTopic)
                    .withKeyDeserializer(LongDeserializer.class)
                    .withValueDeserializer(StringDeserializer.class)

                    .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object)"earliest"))

                    // We're writing to a file, which does not support unbounded data sources. This line makes it bounded to
                    // the first 5 records.
                    // In reality, we would likely be writing to a data source that supports unbounded data, such as BigQuery.
                    .withMaxNumRecords(5)

                    .withoutMetadata() // PCollection<KV<Long, String>>
            )
                    .apply(Values.<String>create())
                    .apply("ExtractWords", ParDo.of(new DoFn<String, String>() {
                        @ProcessElement
                        public void processElement(ProcessContext c) {
                            for (String word : c.element().split(TOKENIZER_PATTERN)) {
                                if (!word.isEmpty()) {
                                    c.output(word);
                                }
                            }
                        }
                    }))
                    .apply(Count.<String>perElement())
                    .apply("FormatResults", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                        @Override
                        public String apply(KV<String, Long> input) {
                            return input.getKey() + ": " + input.getValue();
                        }
                    }))
                    .apply(TextIO.write().to("wordcounts"));

            p.run().waitUntilFinish();

        }
    }
}

