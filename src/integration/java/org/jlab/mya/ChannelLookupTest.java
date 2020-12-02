package org.jlab.mya;

import org.jlab.mya.nexus.DataNexus;
import org.jlab.mya.nexus.OnDemandNexus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

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

        Assert.assertEquals(expectedSize, metadataList.size());
    }
}
