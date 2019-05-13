package io.exp.apachebeam.Model;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface ExecutePipelineOptions extends PipelineOptions {
    @Description("Path of the file to read from")
    public String getInputFile();

    void setInputFile(String value);

    @Validation.Required
    String getOutput();

    void setOutput(String value);


    @Description("BootStrapServer for Kafka")
    public String getBootStrapServer();
    void setBootStrapServer(String value);

    @Description("Input Topic for Kafka")
    public String getInputTopic();
    void setInputTopic(String value);

    @Description("Output Topic for Kafka")
    public String getOutputTopic();
    void setOutputTopic(String value);
/*
    public String getFlinkMaster();

    void setFlinkMaster(String value);*/
}
