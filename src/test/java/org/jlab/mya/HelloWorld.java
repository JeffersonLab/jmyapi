package org.jlab.mya;

import main.org.org.jlab.mya.event.FloatEvent;
import main.org.org.jlab.mya.nexus.OnDemandNexus;
import main.org.org.jlab.mya.params.IntervalQueryParams;
import main.org.org.jlab.mya.service.IntervalService;
import main.org.org.jlab.mya.stream.FloatEventStream;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

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
                = LocalDateTime.parse("2017-01-01T00:00:00.123456").atZone(ZoneId.systemDefault()).toInstant();
        Instant end
                = LocalDateTime.parse("2017-01-01T00:01:00.123456").atZone(ZoneId.systemDefault()).toInstant();

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
