package org.jlab.mya;

import java.util.List;
import org.jlab.mya.nexus.DataNexus;
import org.jlab.mya.nexus.OnDemandNexus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ChannelLookupTest {
  private DataNexus nexus;

  @Before
  public void setUp() {
    nexus = new OnDemandNexus("docker");
  }

  /** Test of findChannel method. */
  @Test
  public void testFindChannel() throws Exception {
    System.out.println("findChannel");
    String q = "%chan%1";
    long limit = 10;
    long offset = 0;

    List<Metadata> metadataList = nexus.findChannel(q, limit, offset);

    for (Metadata metadata : metadataList) {
      System.out.println(metadata);
    }

    long expectedSize = 1;

    Assert.assertEquals(expectedSize, metadataList.size());
  }
}
