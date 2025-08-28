package org.jlab.mya.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.jlab.mya.event.EventCode;
import org.jlab.mya.event.FloatEvent;
import org.junit.Assert;
import org.junit.Test;

public class FloatGraphicalSampleStreamTest {

  @Test
  public void testBasicUsage() throws IOException {
    List<FloatEvent> events = new ArrayList<>();
    events.add(
        new FloatEvent(
            10000000, EventCode.CHANNELS_PRIOR_DATA_MOVED_OFFLINE, 17)); // First, included
    events.add(new FloatEvent(11000000, EventCode.UPDATE, -1)); // min, included
    events.add(new FloatEvent(12000000, EventCode.UPDATE, 11)); // skipped
    events.add(new FloatEvent(13000000, EventCode.UPDATE, 10)); // skipped
    events.add(
        new FloatEvent(14000000, EventCode.UPDATE, 15)); // lttb, max, prior to disconnect, included
    events.add(
        new FloatEvent(15000000, EventCode.NETWORK_DISCONNECTION, 0)); // disconnect, last, included

    List<FloatEvent> exp = new ArrayList<>();
    exp.add(events.get(0));
    exp.add(events.get(1));
    exp.add(events.get(4));
    exp.add(events.get(5));

    List<FloatEvent> result = new ArrayList<>();
    try (EventStream<FloatEvent> stream = new ListStream<>(events, FloatEvent.class)) {
      try (FloatGraphicalSampleStream<FloatEvent> sampleStream =
          new FloatGraphicalSampleStream<>(stream, 1, 6, FloatEvent.class)) {
        FloatEvent e;
        while ((e = sampleStream.read()) != null) {
          result.add(e);
        }
      }
    }
    Assert.assertEquals(exp, result);
  }

  @Test
  public void testBasicUsage2() throws IOException {
    // This is an artificial scenario since EventConde.CHANNELS_... indicates that there are no
    // prior data points,
    // but we include some prior data points.
    List<FloatEvent> events = new ArrayList<>();
    events.add(new FloatEvent(9100000, EventCode.NETWORK_DISCONNECTION, 0)); // disconnect, included
    events.add(new FloatEvent(9200000, EventCode.NETWORK_DISCONNECTION, 0)); // disconnect, included
    events.add(new FloatEvent(9300000, EventCode.NETWORK_DISCONNECTION, 0)); // disconnect, included
    events.add(
        new FloatEvent(
            10000000,
            EventCode.CHANNELS_PRIOR_DATA_MOVED_OFFLINE,
            17)); // First data event, included
    events.add(new FloatEvent(11000000, EventCode.UPDATE, -1)); // min, included
    events.add(new FloatEvent(12000000, EventCode.UPDATE, 11)); // skipped
    events.add(new FloatEvent(13000000, EventCode.UPDATE, 10)); // skipped
    events.add(
        new FloatEvent(14000000, EventCode.UPDATE, 15)); // lttb, max, prior to disconnect, included
    events.add(
        new FloatEvent(15000000, EventCode.NETWORK_DISCONNECTION, 0)); // disconnect, last, included

    List<FloatEvent> exp = new ArrayList<>();
    int[] keepers = {0, 1, 2, 3, 4, 7, 8};
    for (int i : keepers) {
      exp.add(events.get(i));
    }

    List<FloatEvent> result = new ArrayList<>();
    try (EventStream<FloatEvent> stream = new ListStream<>(events, FloatEvent.class)) {
      try (FloatGraphicalSampleStream<FloatEvent> sampleStream =
          new FloatGraphicalSampleStream<>(stream, 1, 6, FloatEvent.class)) {
        FloatEvent e;
        while ((e = sampleStream.read()) != null) {
          result.add(e);
        }
      }
    }
    Assert.assertEquals(exp, result);
  }
}
