package org.jlab.mya;

import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.nexus.DataNexus;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.stream.EventStream;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;

/**
 *
 * @author slominskir
 */
class ConcurrencyTest {

    /**
     * @param args the command line arguments
     * @throws SQLException If an SQL problem arises
     * @throws IOException If an IO problem arises
     */
    public static void main(String[] args) throws SQLException, IOException {

        DataNexus nexus = new OnDemandNexus("history");

        String pv = "R123PMES";
        Instant begin = TimeUtil.toLocalDT("2016-01-01T00:00:00.123456");
        Instant end = TimeUtil.toLocalDT("2017-01-01T00:01:00.123456");

        Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);

        try (EventStream<FloatEvent> stream = nexus.openEventStream(metadata, begin, end)) {

            FloatEvent point = nexus.findEvent(metadata, begin);
            System.out.println("Point value: " + point.toString(6));

            FloatEvent event;

            while ((event = stream.read()) != null) {
                System.out.println("Stream value: " + event.toString(6));
            }
        }
    }

}
