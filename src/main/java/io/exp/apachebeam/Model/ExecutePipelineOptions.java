package io.exp.apachebeam.Model;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface ExecutePipelineOptions extends PipelineOptions {
    public static final int WINDOW_SIZE=10;//in seconds
    @Description("Path of the file to read from")
    public String getInputFile();

    void setInputFile(String value);

    //@Validation.Required
    @Description("Path of the file to write into")
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


    @Description("Fixed window duration, in minutes")
    @Default.Integer(WINDOW_SIZE)
    Integer getWindowSize();

    void setWindowSize(Integer value);
/*
    public String getFlinkMaster();

    void setFlinkMaster(String value);*/
}
