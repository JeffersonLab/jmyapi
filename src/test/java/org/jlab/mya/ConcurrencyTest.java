package org.jlab.mya;

import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.params.IntervalQueryParams;
import org.jlab.mya.params.PointQueryParams;
import org.jlab.mya.stream.FloatEventStream;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.service.IntervalService;
import org.jlab.mya.service.PointService;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;

/**
 *
 * @author slominskir
 */
public class ConcurrencyTest {

    /**
     * @param args the command line arguments
     * @throws SQLException
     * @throws IOException
     */
    public static void main(String[] args) throws SQLException, IOException, InterruptedException {

        DataNexus nexus = new OnDemandNexus("history");
        IntervalService intervalService = new IntervalService(nexus);
        PointService pointService = new PointService(nexus);

        String pv = "R123PMES";
        Instant begin
                = TimeUtil.toLocalDT("2016-01-01T00:00:00.123456");
        Instant end
                = TimeUtil.toLocalDT("2017-01-01T00:01:00.123456");

        Metadata metadata = intervalService.findMetadata(pv);
        IntervalQueryParams intervalParams = new IntervalQueryParams(metadata, begin, end);
        PointQueryParams pointParams = new PointQueryParams(metadata, begin);

        try (FloatEventStream stream = intervalService.openFloatStream(intervalParams)) {

            FloatEvent point = pointService.findFloatEvent(pointParams);
            System.out.println("Point value: " + point.toString(6));

            FloatEvent event;

            while ((event = stream.read()) != null) {
                System.out.println("Stream value: " + event.toString(6));
            }
        }
    }

}
