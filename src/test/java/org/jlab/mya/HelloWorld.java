package org.jlab.mya;

import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.params.IntervalQueryParams;
import org.jlab.mya.service.IntervalService;
import org.jlab.mya.stream.FloatEventStream;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;

/**
 *
 * @author slominskir
 */
public class HelloWorld {

    /**
     * @param args the command line arguments
     * @throws java.sql.SQLException
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws SQLException, IOException {

        DataNexus nexus = new OnDemandNexus("history");
        IntervalService service = new IntervalService(nexus);

        for (String name : DataNexus.getDeploymentNames()) {
            System.out.println(name);
        }
        
        
        String pv = "R123PMES";
        Instant begin
                = TimeUtil.toLocalDT("2017-01-01T00:00:00.123456");
        Instant end
                = TimeUtil.toLocalDT("2017-01-01T00:01:00.123456");

        Metadata metadata = service.findMetadata(pv);
        IntervalQueryParams params = new IntervalQueryParams(metadata, begin, end);
        try (FloatEventStream stream = service.openFloatStream(params)) {

            FloatEvent event;

            while ((event = stream.read()) != null) {
                System.out.println(event.toString(6));
            }
        }
    }

}
