package com.eventfn;
import com.models.GoBike;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;


public class ParseEventFn extends DoFn<String, GoBike> {

    // Log and count parse errors.
    private static final Logger LOG = LoggerFactory.getLogger(ParseEventFn.class);
    private final Counter numParseErrors = Metrics.counter("main", "ParseErrors");

    @ProcessElement
    public void processElement(ProcessContext c) {
        String[] components = c.element().toString().split(",");

        try {
            int durationSec = Integer.parseInt(components[0].replace("\"",""));
            String startTime = components[1].replace("\"","");
            String endTime = components[2].replace("\"","");
            Integer startStationId = components[3].replace("\"","").equals("") ? null : Integer.parseInt(components[3].replace("\"",""));
            String startStationName = components[4].replace("\"","");
            String startStationLatitude = components[5].replace("\"","").equals("") ? null : components[5].replace("\"","");
            String startStationLongitude = components[6].replace("\"","").equals("") ? null : components[6].replace("\"","");
            Integer endStationId = components[7].replace("\"","").equals("") ? null : Integer.parseInt(components[7].replace("\"",""));
            String endStationName = components[8].replace("\"","");
            String endStationLatitude = components[9].replace("\"","").equals("") ? null : components[9].replace("\"","");
            String endStationLongitude = components[10].replace("\"","").equals("") ? null : components[10].replace("\"","");
            int bikerId = components[11].replace("\"","").equals("") ? null : Integer.parseInt(components[11].replace("\"",""));
            String userType = components[12].replace("\"","");
            Integer memeberBirthYear = components[13].replace("\"","").equals("") ? null : Integer.parseInt(components[13].replace("\"",""));
            String memberGender = components[14].replace("\"","");
            Boolean bikeShareForAllTrip = components[15].replace("\"","").equals("") ? null : Boolean.parseBoolean(components[15].replace("\"",""));

            GoBike gInfo = new GoBike(durationSec,  startStationName,  endStationName,  startStationId,  endStationId,  startTime,
                             endTime,  startStationLatitude,  startStationLongitude,  endStationLatitude,  endStationLongitude, bikerId,  userType,
                             memeberBirthYear,  memberGender,  bikeShareForAllTrip);
            c.output(gInfo);
        } catch (Exception e) {
            numParseErrors.inc();
            LOG.info("Parse error on " + c.element() + ", " + e.getMessage());
        }
    }

}

