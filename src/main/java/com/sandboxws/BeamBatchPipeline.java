package com.sandboxws;

import java.util.Arrays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class BeamBatchPipeline {
  public static void main(String[] args) {
    Pipeline pipeline = Pipeline.create();

    // Step 1 - Read CSV file.
    PCollection<String> csvRows = pipeline.apply("Read from CSV", TextIO.read().from("./reviews.csv"));

    // Step 2 - Extract ratings and count them.
    PCollection<KV<String, Long>> ratingsCounts = csvRows
        .apply("Extract Ratings",
            FlatMapElements.into(TypeDescriptors.strings()).via(csvRow -> Arrays.asList(csvRow.split(",")[1])))
        .apply("Count Ratings", Count.<String>perElement());

    // Step 3 - Write results to CSV
    ratingsCounts
        .apply("FormatResults",
            MapElements.into(TypeDescriptors.strings())
                .via((KV<String, Long> ratingsCount) -> ratingsCount.getKey() + " " + ratingsCount.getValue()))
        .apply(TextIO.write().to("./ratings_results").withSuffix(".txt"));

    // Run the pipeline and wait till it finishes before exiting
    pipeline.run().waitUntilFinish();
  }
}
