package org.jlab.mya;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;

import org.jlab.mya.event.IntEvent;
import org.jlab.mya.event.LabeledEnumEvent;
import org.jlab.mya.nexus.DataNexus;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.stream.EventStream;
import org.jlab.mya.stream.LabeledEnumStream;
import org.junit.Test;

/**
 * Test the ExtraInfo query.
 * 
 * @author slominskir
 */
public class ExtraInfoTest {

    @Test
    public void testEnumLabels() throws SQLException, IOException {
        DataNexus nexus = new OnDemandNexus("docker");

        String pv = "channel2";
        Instant begin = TimeUtil.toLocalDT("2019-08-12T00:00:00");
        Instant end = TimeUtil.toLocalDT("2019-08-13T00:00:00");

        Metadata<IntEvent> metadata = nexus.findMetadata(pv, IntEvent.class);
        List<ExtraInfo> infoList = nexus.findExtraInfo(metadata, "enum_strings", null, null);

        for(ExtraInfo info: infoList) {
            System.out.println(info);
            String[] labels = info.getValueAsArray();
            
            for(int i = 0; i < labels.length; i++) {
                String label = labels[i];
                System.out.println(i + ": " + label);
            }
        }
        
        try (EventStream<IntEvent> stream = nexus.openEventStream(metadata, begin, end)) {

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

    @Test
    public void testEnumLabelsSubset() throws SQLException, IOException {
        DataNexus nexus = new OnDemandNexus("docker");

        String pv = "channel2";
        Instant begin = TimeUtil.toLocalDT("2019-08-12T00:00:00");
        Instant end = TimeUtil.toLocalDT("2019-08-13T00:00:00");

        Metadata<IntEvent> metadata = nexus.findMetadata(pv, IntEvent.class);
        List<ExtraInfo> infoList = nexus.findExtraInfo(metadata, "enum_strings", begin, end);

        for(ExtraInfo info: infoList) {
            System.out.println(info);
            String[] labels = info.getValueAsArray();

            for(int i = 0; i < labels.length; i++) {
                String label = labels[i];
                System.out.println(i + ": " + label);
            }
        }
    }
}
