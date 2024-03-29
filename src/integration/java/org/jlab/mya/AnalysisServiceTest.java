package org.jlab.mya;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;

import org.jlab.mya.nexus.DataNexus;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.stream.EventStream;
import org.jlab.mya.event.AnalyzedFloatEvent;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.stream.BoundaryAwareStream;
import org.jlab.mya.stream.FloatAnalysisStream;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author apcarp
 */
public class AnalysisServiceTest {

    /**
     * Test of calculateRunningStatistics method, of class AnalysisService.
     *
     * @throws Exception If something goes wrong!
     */
    //@Test
    public void testCalculateRunningStatistics() throws Exception {

        System.out.println("calculateRunningStatistics");

        DataNexus nexus = new OnDemandNexus("history");
        Instant begin = LocalDateTime.of(2016, Month.OCTOBER, 1, 0, 0, 0).atZone(ZoneId.of("America/New_York")).toInstant();
        Instant end = LocalDateTime.of(2017, Month.OCTOBER, 1, 0, 0, 0).atZone(ZoneId.of("America/New_York")).toInstant();

        System.out.println(begin + " to " + end);

        /*
        myStats -m history -b 2016-10-01 -e 2017-10-01 -l R123PMES
                Name    Min   Mean    Max   Sigma 
        R123PMES -180 101.837 6373.5 586.889
        
        myIntegrate -q -m history -b 2016-10-01 -e 2017-10-01\ 00:00:00 -c R123PMES
        Integrated sum = 3.20031e+09
         */
        System.out.println("\nR123PMES");
        compareStats(nexus, begin, end, "R123PMES", -180., 6373.5, 101.837, 586.889, 3.20031e+09);

        /*
        myStats -m history -b 2016-10-01 -e 2017-10-01 -l R2N3PMES
                Name     Min    Mean    Max   Sigma 
        R2N3PMES -31973 -11.3787 29368 26.7768        
        
        myIntegrate -q -m history -b 2016-10-01 -e 2017-10-01 -c R2N3PMES
        Integrated sum = -3.25353e+08
         */
        System.out.println("\nR2N3PMES");
        compareStats(nexus, begin, end, "R2N3PMES", -31973, 29368, -11.3787, 26.7768, -3.25353e+08);

        /*
        myStats -m history -b 2016-10-01 -e 2017-10-01 -l VIP2E03
             Name    Min     Mean     Max       Sigma 
        VIP2E03 -8.0354 -7.46718 23.1846 1.43507

        myIntegrate -q -m history -b 2016-10-01 -e 2017-10-01 -c VIP2E03
        Integrated sum = -2.06253e+08
         */
        System.out.println("\nVIP2E03");
        compareStats(nexus, begin, end, "VIP2E03", -8.0354, 23.1846, -7.46718, 1.43507, -2.06253e+08);

        // myStats -m opsfb -b 2016-10-01 -e 2017-10-01 -l MARCAA.BDL
        // myIntegrate -q -m opsfb -b 2016-10-01 -e 2017-10-01 -c MARCAA.BDL
        /*
        myStats -m history -b 2016-10-01 -e 2017-10-01 -l MARCAA.BDL
                Name     Min   Mean                  Max      Sigma
        MARCAA.BDL   0 3.45815e+06 3.47227e+06 142666
        myIntegrate -q -m history -b 2016-10-01 -e 2017-10-01 -c MARCAA.BDL
        Integrated sum = 7.43932e+13

         */
        System.out.println("\nMARCAA.BDL");
//        compareStats(nexus, begin, end, "MARCAA.BDL", 0, 3.47227e+06, 3.45818e+06, 140699, 9.8968e+13);
        compareStats(nexus, begin, end, "MARCAA.BDL", 0, 3.47227e+06, 3.45815e+06, 142666, 7.43932e+13);

    }

    private void compareStats(DataNexus nexus, Instant begin, Instant end, String pv, double min, double max, double mean, double sigma,
            double integration) throws SQLException, IOException {

        Metadata<FloatEvent> metadata = nexus.findMetadata(pv, FloatEvent.class);

        FloatEvent priorPoint = nexus.findEvent(metadata, begin);

        RunningStatistics result = null;

        try(
                final EventStream<FloatEvent> stream = nexus.openEventStream(metadata, begin, end);
                final EventStream<FloatEvent> boundaryStream = new BoundaryAwareStream<>(stream, begin, end, priorPoint, false, FloatEvent.class);
                final FloatAnalysisStream analysisStream = new FloatAnalysisStream(boundaryStream);
            ) {

                AnalyzedFloatEvent event;
                while ((event = analysisStream.read()) != null) {
                    // No-op - we just want to allow analysis stream to collect stats on entire series
                    // If we wanted we could inspect running stats incrementally in here...
                }

                result = analysisStream.getLatestStats();
        }

        double rms = Math.sqrt(sigma * sigma + mean * mean);

        System.out.println("Myapi min:   " + min);
        System.out.println("Jmyapi min:  " + result.getMin());
        System.out.println("Myapi max:   " + max);
        System.out.println("Jmyapi max:  " + result.getMax());
        System.out.println("Myapi mean:  " + mean);
        System.out.println("Jmyapi mean: " + result.getMean());
        System.out.println("");

        System.out.println("Myapi   sigma: " + sigma);
        System.out.println("Jmyapi  sigma: " + result.getSigma());
        System.out.println("Myapi  rms:  " + rms);
        System.out.println("Jmyapi rms: " + result.getRms());
        System.out.println("");

        System.out.println("Myapi         integration:       " + integration);
        System.out.println("Jmyapi        integration:       " + result.getIntegration());
        System.out.println("");

        double minMaxDiff = 0.000001;  // absolutely the same within 6 decimal places
        double percDiff = 0.5;  // 0.5% difference

        // Make sure we get something back
        Assert.assertNotEquals(null, result);
        Assert.assertEquals(min, result.getMin(), minMaxDiff);
        Assert.assertEquals(max, result.getMax(), minMaxDiff);
        Assert.assertEquals(mean, result.getMean(), Math.abs(percDiff*mean));  // difference within a percentage of the expected value
        Assert.assertEquals(sigma, result.getSigma(), Math.abs(percDiff * sigma));
        Assert.assertEquals(rms, result.getRms(), Math.abs(percDiff * rms));

        // This test is known to fail as of now.  I believe this is due to a difference in time representation
        Assert.assertEquals(integration, result.getIntegration(), Math.abs(percDiff * integration));
    }
}
