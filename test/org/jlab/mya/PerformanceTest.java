package org.jlab.mya;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.stream.FloatEventStream;

/**
 *
 * @author ryans
 */
public class PerformanceTest {
    /**
     * @param args the command line arguments
     * @throws java.sql.SQLException
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws SQLException, IOException {

        DataNexus nexus = new OnDemandNexus(Deployment.ops);
        QueryService service = new QueryService(nexus);

        String pv = "R123PMES";
        Instant begin
                = LocalDateTime.parse("2016-08-22T08:43:00").atZone(ZoneId.systemDefault()).toInstant();
        Instant end
                = LocalDateTime.parse("2017-07-22T08:43:28").atZone(ZoneId.systemDefault()).toInstant();

        Metadata metadata = service.findMetadata(pv);
        
        QueryParams params = new QueryParams(metadata, begin, end);
        
        long count = service.count(params);
        System.out.println("count: " + count);
        /*try (FloatEventStream stream = service.openFloat(params)) {

            FloatEvent event;

            while ((event = stream.read()) != null) {
                System.out.println(event);
            }
        }*/
    }    
}
