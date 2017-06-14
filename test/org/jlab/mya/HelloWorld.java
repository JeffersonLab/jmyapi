package org.jlab.mya;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.stream.FloatEventStream;

/**
 *
 * @author ryans
 */
public class HelloWorld {

    /**
     * @param args the command line arguments
     * @throws java.lang.ClassNotFoundException
     * @throws java.sql.SQLException
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {

        //System.out.println(MyaUtil.fromMyaTimestamp(6276360291443298313L).atZone(ZoneId.systemDefault()));
        DataNexus nexus = new OnDemandNexus(Deployment.ops);
        QueryService service = new QueryService(nexus);

        //String pv = "DCPHP2ADC10";
        String pv = "measureQ:heatlocked0031";
        Instant begin
                = LocalDateTime.parse("2016-08-22T08:43:00").atZone(ZoneId.systemDefault()).toInstant();
        Instant end
                = LocalDateTime.parse("2017-07-22T08:43:28").atZone(ZoneId.systemDefault()).toInstant();

        Metadata metadata = service.findMetadata(pv);
        QueryParams params = new QueryParams(metadata, begin, end);
        try (FloatEventStream stream = service.openFloat(params)) {

            FloatEvent event;

            while ((event = stream.read()) != null) {
                System.out.println(event);
            }
        }
    }

}
