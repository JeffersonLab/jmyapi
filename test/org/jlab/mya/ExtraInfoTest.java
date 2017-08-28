package org.jlab.mya;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import org.jlab.mya.event.LabeledEnumEvent;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.params.IntervalQueryParams;
import org.jlab.mya.service.IntervalService;
import org.jlab.mya.stream.IntEventStream;
import org.jlab.mya.stream.wrapped.LabeledEnumStream;
import org.junit.Test;

/**
 * Test the ExtraInfo query.
 * 
 * @author slominskir
 */
public class ExtraInfoTest {

    @Test
    public void testEnumLabels() throws SQLException, IOException {
        DataNexus nexus = new OnDemandNexus(Deployment.ops);
        IntervalService service = new IntervalService(nexus);

        String pv = "IPMBMOD";
        Instant begin
                = LocalDateTime.parse("2007-01-01T00:00:00").atZone(ZoneId.systemDefault()).toInstant();
        Instant end
                = LocalDateTime.parse("2017-01-01T00:00:00").atZone(ZoneId.systemDefault()).toInstant();

        Metadata metadata = service.findMetadata(pv);
        List<ExtraInfo> infoList = service.findExtraInfo(metadata, "enum_strings");

        for(ExtraInfo info: infoList) {
            System.out.println(info);
            String[] labels = info.getValueAsArray();
            
            for(String label: labels) {
                System.out.println(label);
            }
        }
        
        IntervalQueryParams params = new IntervalQueryParams(metadata, begin, end);   
        
        try (IntEventStream stream = service.openIntStream(params)) {

            /*IntEvent event;

            while ((event = stream.read()) != null) {
                System.out.println(event.toString(0, infoList.get(0).getValueAsArray()));
            }*/
            
            LabeledEnumEvent event;
            
            LabeledEnumStream les = new LabeledEnumStream(stream, infoList);
            
            while((event = les.read()) != null) {
                System.out.println(event);
            }
        }        
    }
}
