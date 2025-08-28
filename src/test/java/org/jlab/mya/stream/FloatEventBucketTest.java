package org.jlab.mya.stream;

import static org.junit.Assert.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import org.jlab.mya.TimeUtil;
import org.jlab.mya.event.EventCode;
import org.jlab.mya.event.FloatEvent;
import org.junit.Test;

/**
 * @author apcarp
 */
public class FloatEventBucketTest {

  /** Test of downSample and getDownSample method, of class FloatEventBucket. */
  @Test
  public void testDownSampleAndGetDownSample() {
    System.out.println("downSample");
    Instant begin = TimeUtil.toLocalDT("2017-03-01T00:00:00");
    Instant end = TimeUtil.toLocalDT("2017-03-02T00:00:00");

    FloatEvent e1 =
        new FloatEvent(begin.minusSeconds(1), EventCode.UPDATE, 1.0f); // just before bucket
    FloatEvent e3 = new FloatEvent(end.plusSeconds(1), EventCode.UPDATE, 0.0f); // just after bucket

    List<FloatEvent> events = new ArrayList<>();

    // Include the ORIGIN_... event to make sure it gets treated like a regular update and is
    // skipped.
    events.add(
        new FloatEvent(
            begin.plusMillis(500),
            EventCode.ORIGIN_OF_CHANNELS_HISTORY,
            0.5f)); // non-disconnect, should be skipped
    events.add(
        new FloatEvent(
            begin.plusSeconds(1),
            EventCode.UPDATE,
            0.6f)); // update, included because it precedes a disconnect event
    events.add(
        new FloatEvent(begin.plusSeconds(2), EventCode.NETWORK_DISCONNECTION, 0.0f)); // disconnect
    events.add(
        new FloatEvent(begin.plusSeconds(3), EventCode.ARCHIVER_SHUTDOWN, 0.0f)); // disconnect
    events.add(
        new FloatEvent(
            begin.plusSeconds(4), EventCode.ARCHIVING_OF_CHANNEL_TURNED_OFF, 0.0f)); // disconnect
    events.add(new FloatEvent(begin.plusSeconds(5), EventCode.UPDATE, 10.0f)); // max
    events.add(
        new FloatEvent(begin.plusSeconds(3998), EventCode.UPDATE, 0.9f)); // Should be skipped
    events.add(
        new FloatEvent(
            begin.plusSeconds(3999),
            EventCode.UPDATE,
            1.9f)); // Included because it precedes a non-update
    events.add(
        new FloatEvent(begin.plusSeconds(4000), EventCode.NAN_OR_INFINITY, 0.0f)); // disconnect
    events.add(
        new FloatEvent(
            begin.plusSeconds(5000),
            EventCode.UPDATE,
            2.0f)); // Included since it follows a non-update event
    events.add(
        new FloatEvent(
            begin.plusSeconds(43200), EventCode.UPDATE, 9.99f)); // lttb with given e1, e3
    events.add(
        new FloatEvent(begin.plusSeconds(43201), EventCode.UPDATE, 0.9f)); // Should be skipped
    events.add(
        new FloatEvent(begin.plusSeconds(43202), EventCode.UPDATE, 1.9f)); // Should be skipped
    events.add(
        new FloatEvent(
            begin.plusSeconds(43203),
            EventCode.UPDATE,
            9.9f)); // Included since it precedes a non-update event
    events.add(
        new FloatEvent(
            begin.plusSeconds(47200), EventCode.NETWORK_DISCONNECTION, 0.0f)); // non-update
    events.add(
        new FloatEvent(
            begin.plusSeconds(50000),
            EventCode.UPDATE,
            -2.1f)); // Included since it follows a non-update event
    events.add(new FloatEvent(end.minusSeconds(5), EventCode.UPDATE, 3.3f)); // should be skipped
    events.add(new FloatEvent(end.minusSeconds(1), EventCode.UPDATE, -5.1f)); // min

    FloatEventBucket<FloatEvent> instance = new FloatEventBucket<>(events);
    FloatEvent expLTTB = events.get(10);
    FloatEvent resultLTTB = instance.downSample(e1, e3);
    SortedSet<FloatEvent> expSet = new TreeSet<>();
    // Added the lists
    int[] keepers = {1, 2, 3, 4, 5, 7, 8, 9, 10, 13, 14, 15, 17};
    for (int i : keepers) {
      expSet.add(events.get(i));
    }
    SortedSet<FloatEvent> resultSet = instance.getDownSampledOutput();
    assertEquals(expLTTB, resultLTTB);
    assertEquals(expSet, resultSet);
  }

  /** Test of calculateTriangleArea method, of class FloatEventBucket. */
  @Test
  public void testCalculateTriangleArea() {
    System.out.println("calculateTriangleArea");
    List<FloatEvent> events = new ArrayList<>();
    Instant now = Instant.now();

    // Triangle 1 - area = 5.   ORIGIN should be treated same as UPDATE
    events.add(new FloatEvent(now, EventCode.ORIGIN_OF_CHANNELS_HISTORY, 0.0f));
    events.add(new FloatEvent(now.minusSeconds(5), EventCode.UPDATE, 1.0f));
    events.add(new FloatEvent(now.minusSeconds(10), EventCode.UPDATE, 0.0f));

    // Triangle 2 - should be identical to triangle 1
    events.add(new FloatEvent(now.minusSeconds(10000), EventCode.UPDATE, 0.0f));
    events.add(new FloatEvent(now.minusSeconds(10005), EventCode.UPDATE, 1.0f));
    events.add(new FloatEvent(now.minusSeconds(10010), EventCode.UPDATE, 0.0f));

    // Triangle 3 -
    events.add(new FloatEvent(now.minusSeconds(1000000000), EventCode.UPDATE, 50.0f));
    events.add(new FloatEvent(now.minusSeconds(1050000000), EventCode.UPDATE, 1.0f));
    events.add(new FloatEvent(now.minusSeconds(1100000000), EventCode.UPDATE, 0.0f));

    double expResult1 = 5;
    double expResult2 = 5;
    double expResult3 = 1200000000;
    double result1 =
        FloatEventBucket.calculateTriangleArea(events.get(0), events.get(1), events.get(2));
    double result2 =
        FloatEventBucket.calculateTriangleArea(events.get(3), events.get(4), events.get(5));
    double result3 =
        FloatEventBucket.calculateTriangleArea(events.get(6), events.get(7), events.get(8));

    // Not sure how much rounding error will occur - that should be plenty close enough.
    assertEquals(expResult1, result1, 0.00000001);
    assertEquals(expResult2, result2, 0.00000001);
    assertEquals(expResult3, result3, 0.00001);
  }
}
