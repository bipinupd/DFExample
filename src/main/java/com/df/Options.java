package com.df;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * Options supported by the exercise pipelines.
 */
public interface Options extends PipelineOptions {

    @Description("Path to the data file(s) containing data.")
    String getInput();

    void setInput(String value);

    @Description("BigQuery Dataset to write tables to. Must already exist.")
    @Validation.Required
    String getOutputDataset();

    void setOutputDataset(String value);

    @Description("The BigQuery table name. Should not already exist.")
    @Validation.Required
    String getOutputTableName();

    void setOutputTableName(String value);
}
