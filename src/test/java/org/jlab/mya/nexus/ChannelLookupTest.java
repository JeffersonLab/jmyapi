package org.jlab.mya.nexus;

import org.jlab.mya.Metadata;
import org.jlab.mya.TimeUtil;
import org.jlab.mya.event.FloatEvent;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ChannelLookupTest {
    private DataNexus nexus;

    @Before
    public void setUp() {
        nexus = new OnDemandNexus("history");
    }

    /**
     * Test of findChannel method.
     */
    @Test
    public void testFindChannel() throws Exception {
        System.out.println("findChannel");
        String q = "%R123%";
        long limit = 10;
        long offset = 0;

        List<Metadata> metadataList = nexus.findChannel(q, limit, offset);

        for(Metadata metadata: metadataList) {
            System.out.println(metadata);
        }

        long expectedSize = 10;

        assertEquals(expectedSize, metadataList.size());
    }
}
