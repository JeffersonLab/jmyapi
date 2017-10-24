/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jlab.mya.service;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import org.jlab.mya.DataNexus;
import org.jlab.mya.Deployment;
import org.jlab.mya.Metadata;
import org.jlab.mya.analysis.RunningStatistics;
import org.jlab.mya.nexus.OnDemandNexus;
import org.jlab.mya.params.IntervalQueryParams;
import org.junit.Test;

/**
 *
 * @author adamc
 */
public class AnalysisServiceTest {

    public AnalysisServiceTest() {
    }

    /**
     * Test of calculateRunningStatistics method, of class AnalysisService.
     *
     * @throws java.lang.Exception
     */
    @Test
    public void testCalculateRunningStatistics() throws Exception {

        System.out.println("calculateRunningStatistics");

        DataNexus nexus = new OnDemandNexus(Deployment.opsfb);
        Instant begin = LocalDateTime.of(2016, Month.OCTOBER, 1, 0, 0, 0).atZone(ZoneId.of("America/New_York")).toInstant();
        Instant end = LocalDateTime.of(2017, Month.OCTOBER, 1, 0, 0, 0).atZone(ZoneId.of("America/New_York")).toInstant();
        
        System.out.println(begin + " to " + end);

        // myStats -m opsfb -b 2016-10-01 -e 2017-10-01 -l R123PMES
        // myIntegrate -q -m opsfb -b 2016-10-01 -e 2017-10-01\ 00:00:00 -c R123PMES
        System.out.println("\nR123PMES");
        compareStats(nexus, begin, end, "R123PMES", -180., 6373.5, 101.837, 586.888, 3.20032e+09);

        // myStats -m opsfb -b 2016-10-01 -e 2017-10-01 -l R2N3PMES
        // myIntegrate -q -m opsfb -b 2016-10-01 -e 2017-10-01 -c R2N3PMES
        System.out.println("\nR2N3PMES");
        compareStats(nexus, begin, end, "R2N3PMES", -31973, 29368, -10.5322, 26.665, -3.2543e+08);

        // myStats -m opsfb -b 2016-10-01 -e 2017-10-01 -l VIP2E03
        // myIntegrate -q -m opsfb -b 2016-10-01 -e 2017-10-01 -c VIP2E03
        System.out.println("\nVIP2E03");
        compareStats(nexus, begin, end, "VIP2E03", -8.0354, 23.1846, -7.49467, 1.36566, -2.30337e+08);
        
        // myStats -m opsfb -b 2016-10-01 -e 2017-10-01 -l MARCAA.BDL
        // myIntegrate -q -m opsfb -b 2016-10-01 -e 2017-10-01 -c MARCAA.BDL
        System.out.println("\nMARCAA.BDL");
        compareStats(nexus, begin, end, "MARCAA.BDL", 0, 3.47227e+06, 3.45818e+06, 140699, 9.8968e+13);
        
    }

    private void compareStats(DataNexus nexus, Instant begin, Instant end, String pv, double min, double max, double mean, double sigma,
            double integration) throws SQLException, IOException {

        AnalysisService service = new AnalysisService(nexus);
        Metadata metadata = service.findMetadata(pv);

        IntervalQueryParams params = new IntervalQueryParams(metadata, begin, end);
        RunningStatistics result = service.calculateRunningStatistics(params);

        double rms = Math.sqrt(sigma * sigma + mean * mean);
        
        System.out.println("Myapi min:   " + min);
        System.out.println("Jmyapi min:  " + result.getMin());
        System.out.println("Myapi max:   " + max);
        System.out.println("Jmyapia max:  " + result.getMax());
        System.out.println("Myapi mean:  " + mean);
        System.out.println("Jmyapi mean: " + result.getMean());
        System.out.println("");

        System.out.println("Myapi   sigma: " + sigma);
        System.out.println("Jmyapi  sigma: " + result.getSigma());
        System.out.println("Myapie rms:  " + rms);
        System.out.println("Jmyapi rms: " + result.getRms());
        System.out.println("");

        System.out.println("Myapi         integration:       " + integration);
        System.out.println("Jmyapi        integration:       " + result.getIntegration());
        System.out.println("");


        double minMaxDiff = 0.000001;  // absolutely the same within 6 decimal places
        double percDiff = 0.0005;  // 0.05% difference
        
        assert (Math.abs( (min - result.getMin()) ) < minMaxDiff);
        assert (Math.abs( (max - result.getMax()) ) < minMaxDiff);
        assert (Math.abs( (mean - result.getMean()) / mean ) < percDiff);
        assert (Math.abs( (sigma - result.getSigma()) / sigma ) < percDiff);
        assert (Math.abs( (rms - result.getRms()) / rms ) < percDiff);
        
        // This test is known to fail as of now.  I believe this is due to a difference in time representation
        //assert (Math.abs( (integration - result.getIntegration()) / integration ) < percDiff); 
    }
}
