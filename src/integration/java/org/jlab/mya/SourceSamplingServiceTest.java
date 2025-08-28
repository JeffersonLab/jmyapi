package org.jlab.mya;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.nexus.DataNexus;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.stream.EventStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for SourceSamplingService. Sampling done in-database. See also:
 * stream.wrapped.ApplicationLevelSamplingTest.
 *
 * @author slominskir
 */
public class SourceSamplingServiceTest {

  private DataNexus nexus;

  @Before
  public void setUp() {
    nexus = new OnDemandNexus("docker");
  }

  /**
   * Test naive sampler.
   *
   * <p>Compare with "myget -l 24 -c IGL1I00POTcurrent -b 2019-08-12 -e 2019-08-13 -f 6"
   */
  @Test
  public void testMyGetSampler() throws Exception {

    String pv = "channel1";
    Instant begin = TimeUtil.toLocalDT("2019-08-12T00:00:00");
    Instant end = TimeUtil.toLocalDT("2019-08-13T00:00:00");
    long limit = 24;
    int fractionalDigits = 6; // microseconds; seems to be max precision of myget

    Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);

    long expSize = 24; // We limit to 24, but we know historical data only has 21
    List<FloatEvent> eventList = new ArrayList<>();
    try (EventStream<FloatEvent> stream =
        nexus.openMyGetSampleStream(metadata, begin, end, limit)) {
      FloatEvent event;
      while ((event = stream.read()) != null) {
        eventList.add(event);
        // System.out.println(event.toString(fractionalDigits));
      }
    }
    Assert.assertEquals(expSize, eventList.size());
  }
}
