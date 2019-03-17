package com.df;

import com.google.api.services.bigquery.model.TableReference;
import com.eventfn.FormatEventFunction;
import com.eventfn.ParseEventFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;

public class DFMain {
    public static void main(String[] args) throws Exception {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        TableReference tableRef = new TableReference();
        tableRef.setDatasetId(options.getOutputDataset());
        tableRef.setProjectId(options.as(GcpOptions.class).getProject());
        tableRef.setTableId(options.getOutputTableName());

        // Read events from a CSV file, parse them and write (import) them to BigQuery.
        pipeline
                .apply(TextIO.read().from(options.getInput()))
                .apply("ParseEvent", ParDo.of(new ParseEventFn()))
                .apply("FormatEventData", ParDo.of(new FormatEventFunction()))
                .apply(
                        BigQueryIO.writeTableRows().to(tableRef)
                                .withSchema(FormatEventFunction.getSchema())
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pipeline.run();
    }

}

