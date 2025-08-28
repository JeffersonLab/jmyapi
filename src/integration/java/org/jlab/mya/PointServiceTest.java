package org.jlab.mya;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.jlab.mya.event.EventCode;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.event.IntEvent;
import org.jlab.mya.event.MultiStringEvent;
import org.jlab.mya.nexus.DataNexus;
import org.jlab.mya.nexus.OnDemandNexus;
import org.junit.*;

/**
 * Test the point service.
 *
 * @author slominskir
 */
public class PointServiceTest {

  private DataNexus nexus;

  @Before
  public void setUp() {
    nexus = new OnDemandNexus("docker");
  }

  /**
   * Test of findFloatEvent method, of class PointService.
   *
   * <pre>
   * Compare with:
   * myget -c R123PMES -m history -t "2017-01-01 00:00:05"
   *
   * mysql -A -h hstmya1 -u myapi -D archive -p
   * select * from table_32108 force index for order by (primary) where time <= 398156032460718080 order by time desc limit 1;
   * </pre>
   *
   * <p>Note: without query hint: "force index for order by (primary)" this unit test takes over 25
   * seconds vs less than 0.5 seconds
   */
  @Test
  public void testFindFloatEvent() throws Exception {
    System.out.println("findFloatEvent");
    String pv = "channel1";
    Instant timestamp = TimeUtil.toLocalDT("2019-08-12T00:00:05");

    System.out.println("begin in myatime format: " + TimeUtil.toMyaTimestamp(timestamp));

    long start = System.currentTimeMillis();

    Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);

    long stop = System.currentTimeMillis();

    System.out.println("Metadata lookup Took: " + (stop - start) / 1000.0 + " seconds");

    System.out.println(metadata);

    float expResult = 95.21179962158203f;

    start = System.currentTimeMillis();

    FloatEvent result = nexus.findEvent(metadata, timestamp);

    stop = System.currentTimeMillis();

    System.out.println("Point lookup Took: " + (stop - start) / 1000.0 + " seconds");

    System.out.println(result);

    Assert.assertEquals(expResult, result.getValue(), 0.01);
  }

  /** Test of findIntEvent method, of class PointService. */
  @Test
  public void testFindIntEvent() throws Exception {
    System.out.println("findIntEvent");

    String pv = "channel2";
    Instant lessThanOrEqual = TimeUtil.toLocalDT("2019-08-12T01:00:00");
    Instant actual = TimeUtil.toLocalDT("2019-08-12T00:44:11.532879147");
    Metadata<IntEvent> metadata = nexus.findMetadata(pv, IntEvent.class);

    IntEvent expResult = new IntEvent(actual, EventCode.UPDATE, 3);
    IntEvent result = nexus.findEvent(metadata, lessThanOrEqual);

    System.out.println(result.getTimestampAsInstant().atZone(ZoneId.of("America/New_York")));

    Assert.assertEquals(expResult.getTimestamp(), result.getTimestamp(), 0.0000001);
    Assert.assertEquals(expResult.getValue(), result.getValue());
    Assert.assertEquals(expResult.getCode(), result.getCode());
  }

  /** Test of findMultiStringEvent method, of class PointService. */
  @Test
  public void testFindMultiStringEvent() throws Exception {
    System.out.println("findMultiStringEvent");
    String pv = "channel3";
    Instant lessThanOrEqual =
        Instant.from(ZonedDateTime.of(2019, 8, 12, 1, 0, 0, 0, ZoneId.of("America/New_York")));
    Instant actual =
        Instant.from(
            ZonedDateTime.of(2019, 8, 12, 0, 0, 1, 418386347, ZoneId.of("America/New_York")));

    String value =
        "1565580000,"
            + "1565580000,"
            + "1565580000,"
            + "1565570000,"
            + "1565570000,"
            + "1565560000,"
            + "1565560000,"
            + "1565560000,"
            + "1565550000,"
            + "1565550000,"
            + "1565550000,"
            + "1565540000,"
            + "1565540000,"
            + "1565540000,"
            + "1565530000,"
            + "1565530000,"
            + "1565520000,"
            + "1565520000,"
            + "1565520000,"
            + "1565510000,"
            + "1565510000,"
            + "1565510000,"
            + "1565500000,"
            + "1565500000,"
            + "1565500000,"
            + "1565490000,"
            + "1565490000,"
            + "1565490000,"
            + "1565480000,"
            + "1565480000,"
            + "1565470000,"
            + "1565470000,"
            + "1565470000,"
            + "1565460000,"
            + "1565460000,"
            + "1565460000,"
            + "1565450000,"
            + "1565450000,"
            + "1565450000,"
            + "1565440000,"
            + "1565440000,"
            + "1565430000,"
            + "1565430000,"
            + "1565430000,"
            + "1565420000,"
            + "1565420000,"
            + "1565420000,"
            + "1565410000,"
            + "1565410000,"
            + "1565410000,"
            + "1565400000,"
            + "1565400000,"
            + "1565400000,"
            + "1565390000,"
            + "1565390000,"
            + "1565380000,"
            + "1565380000,"
            + "1565380000,"
            + "1565370000,"
            + "1565370000,"
            + "1565370000,"
            + "1565360000,"
            + "1565360000,"
            + "1565360000,"
            + "1565350000,"
            + "1565350000,"
            + "1565340000,"
            + "1565340000,"
            + "1565340000,"
            + "1565330000,"
            + "1565330000,"
            + "1565330000,"
            + "1565320000,"
            + "1565320000,"
            + "1565320000,"
            + "1565310000,"
            + "1565310000,"
            + "1565310000,"
            + "1565300000,"
            + "1565300000,"
            + "1565290000,"
            + "1565290000,"
            + "1565290000,"
            + "1565280000,"
            + "1565280000,"
            + "1565280000,"
            + "1565270000,"
            + "1565270000,"
            + "1565270000,"
            + "1565260000,"
            + "1565260000,"
            + "1565250000,"
            + "1565250000,"
            + "1565250000,"
            + "1565240000,"
            + "1565240000,"
            + "1565240000,"
            + "1565230000,"
            + "1565230000,"
            + "1565230000,"
            + "1565220000,"
            + "1565220000,"
            + "1565220000,"
            + "1565210000,"
            + "1565210000,"
            + "1565200000,"
            + "1565200000,"
            + "1565200000,"
            + "1565190000,"
            + "1565190000,"
            + "1565190000,"
            + "1565180000,"
            + "1565180000,"
            + "1565180000,"
            + "1565170000,"
            + "1565170000,"
            + "1565160000,"
            + "1565160000,"
            + "1565160000,"
            + "1565150000,"
            + "1565150000,"
            + "1565150000,"
            + "1565140000,"
            + "1565140000,"
            + "1565140000,"
            + "1565130000,"
            + "1565130000,"
            + "1565130000,"
            + "1565120000,"
            + "1565120000,"
            + "1565110000,"
            + "1565110000,"
            + "1565110000,"
            + "1565100000,"
            + "1565100000,"
            + "1565100000,"
            + "1565090000,"
            + "1565090000,"
            + "1565090000,"
            + "1565080000,"
            + "1565080000,"
            + "1565070000,"
            + "1565070000,"
            + "1565070000,"
            + "1565060000,"
            + "1565060000,"
            + "1565060000,"
            + "1565050000,"
            + "1565050000,"
            + "1565050000,"
            + "1565040000,"
            + "1565040000,"
            + "1565040000,"
            + "1565030000,"
            + "1565030000,"
            + "1565020000,"
            + "1565020000,"
            + "1565020000,"
            + "1565010000,"
            + "1565010000,"
            + "1565010000,"
            + "1565000000,"
            + "1565000000,"
            + "1565000000,"
            + "1564990000,"
            + "1564990000,"
            + "1564980000,"
            + "1564980000";

    MultiStringEvent expResult = new MultiStringEvent(actual, EventCode.UPDATE, value.split(","));

    Metadata<MultiStringEvent> metadata = nexus.findMetadata(pv, MultiStringEvent.class);

    MultiStringEvent result = nexus.findEvent(metadata, lessThanOrEqual);
    System.out.println(result.getTimestampAsInstant().atZone(ZoneId.of("America/New_York")));
    for (String v : result.getValue()) {
      System.out.println(v);
    }

    Assert.assertEquals(
        expResult.getTimestampAsInstant().getEpochSecond(),
        result.getTimestampAsInstant().getEpochSecond());
    Assert.assertArrayEquals(expResult.getValue(), result.getValue());
  }
}
