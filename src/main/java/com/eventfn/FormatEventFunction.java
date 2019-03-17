package com.eventfn;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.models.GoBike;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.utils.TableSchemaUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

public class FormatEventFunction extends DoFn<GoBike, TableRow> {
    private static final Logger LOG = LoggerFactory.getLogger(FormatEventFunction.class);
    private final Counter numFormatErrors = Metrics.counter("main", "ParseErrors");
    @DoFn.ProcessElement
    public void processElement(ProcessContext c) {
        try {
            GoBike event = (GoBike) c.element();
            TableRow row = new TableRow()
                    .set("duration_sec", event.getDurationSec())
                    .set("start_time",event.getStartTime())
                    .set("end_time", event.getEndTime())
                    .set("start_station_id", event.getStartStationId())
                    .set("start_station_name", event.getStartStationName())
                    .set("start_station_latitude", event.getStartStationLatitude())
                    .set("start_station_longitude", event.getStartStationLongitude())
                    .set("end_station_id", event.getEndStationId())
                    .set("end_station_name", event.getEndStationName())
                    .set("end_station_latitude", event.getEndStationLatitude())
                    .set("end_station_longitude", event.getEndStationLongitude())
                    .set("bike_id", event.getBikerId())
                    .set("user_type", event.getUserType())
                    .set("member_birth_year", event.getMemeberBirthYear())
                    .set("member_gender", event.getMemberGender())
                    .set("bike_share_for_all_trip", event.getBikeShareForAllTrip());
            c.output(row);
        }catch (Exception e){
            numFormatErrors.inc();
            LOG.info("FormatEventFunction " + c.element() + ", " + e.getMessage());
        }
    }

    /**
     * Defines the BigQuery schema.
     */
    public static TableSchema getSchema() throws  Exception{
        ClassLoader classLoader = FormatEventFunction.class.getClassLoader();
        File file = new File(classLoader.getResource("gobikeschema.json").getFile());
        InputStream io = new FileInputStream(file);
        TableSchemaUtils tableSchemaUtils= new TableSchemaUtils();
        TableSchema schema = tableSchemaUtils.readSchema(io);
        return schema;
    }
}
