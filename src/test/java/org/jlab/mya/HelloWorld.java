package org.jlab.mya;

import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.nexus.DataNexus;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.params.IntervalQueryParams;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;

/**
 *
 * @author slominskir
 */
public class HelloWorld {

    /**
     * Entry point of the application.
     *
     * @param args the command line arguments
     * @throws SQLException If unable to query the SQL database
     * @throws IOException If unable to stream data
     */
    public static void main(String[] args) throws SQLException, IOException {

        DataNexus nexus = new OnDemandNexus("history");

        for (String name : DataNexus.getDeploymentNames()) {
            System.out.println(name);
        }
        
        
        String pv = "R123PMES";
        Instant begin = TimeUtil.toLocalDT("2017-01-01T00:00:00.123456");
        Instant end = TimeUtil.toLocalDT("2017-01-01T00:01:00.123456");

        Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);
        try (EventStream<FloatEvent> stream = nexus.openEventStream(metadata, begin, end)) {

            FloatEvent event;

            while ((event = stream.read()) != null) {
                System.out.println(event.toString(6));
            }
        }
    }

}
