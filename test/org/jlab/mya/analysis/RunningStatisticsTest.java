/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jlab.mya.analysis;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import org.jlab.mya.EventCode;
import org.jlab.mya.event.FloatEvent;
import org.junit.Test;

/**
 *
 * @author adamc
 */
public class RunningStatisticsTest {

    double delta = 0.000001;
    
    public RunningStatisticsTest() {
    }

    /**
     * Test of reset method, of class RunningStatistics.
     */
    @Test
    public void testReset() {
        System.out.println("reset");

        RunningStatistics rs = new RunningStatistics();
        Instant t1 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 0, 0, 0).toInstant(ZoneOffset.UTC);
        Instant t2 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 10, 0, 0).toInstant(ZoneOffset.UTC);
        Instant t3 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 20, 0, 0).toInstant(ZoneOffset.UTC);
        Instant t4 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 30, 0, 0).toInstant(ZoneOffset.UTC);

        rs.push(new FloatEvent(t1, EventCode.UPDATE, 1.f));
        rs.push(new FloatEvent(t2, EventCode.UPDATE, 2.f));
        rs.push(new FloatEvent(t3, EventCode.UPDATE, 3.f));
        rs.push(new FloatEvent(t4, EventCode.UPDATE, 4.f));

        // The mean should have been 2 before the reset and null after.  Check both to make sure things are working as expected.
        Double mean = rs.getMean();
        rs.reset();
        assert (mean.equals(2.) && rs.getMean() == null);
    }

    /**
     * Test of push method, of class RunningStatistics.
     */
    @Test
    public void testPush() {
        System.out.println("push");

        RunningStatistics rs = new RunningStatistics();
        Instant t1 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 0, 0, 0).toInstant(ZoneOffset.UTC);
        Instant t2 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 10, 0, 0).toInstant(ZoneOffset.UTC);
        Instant t3 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 20, 0, 0).toInstant(ZoneOffset.UTC);
        Instant t4 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 30, 0, 0).toInstant(ZoneOffset.UTC);

        rs.push(new FloatEvent(t1, EventCode.UPDATE, 1.f));
        rs.push(new FloatEvent(t2, EventCode.UPDATE, 2.f));
        rs.push(new FloatEvent(t3, EventCode.UPDATE, 3.f));
        rs.push(new FloatEvent(t4, EventCode.UPDATE, 4.f));

        assert(rs.getDuration().equals(1800.));
    }

    /**
     * Test of getMin method, of class RunningStatistics.
     */
    @Test
    public void testGetMin() {
        System.out.println("getMin");

        RunningStatistics rs = new RunningStatistics();
        Instant t1 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 0, 0, 0).toInstant(ZoneOffset.UTC);
        Instant t2 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 10, 0, 0).toInstant(ZoneOffset.UTC);
        Instant t3 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 20, 0, 0).toInstant(ZoneOffset.UTC);
        Instant t4 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 30, 0, 0).toInstant(ZoneOffset.UTC);

        rs.push(new FloatEvent(t1, EventCode.UPDATE, 1.f));
        rs.push(new FloatEvent(t2, EventCode.UPDATE, 2.f));
        rs.push(new FloatEvent(t3, EventCode.UPDATE, -3.f));
        rs.push(new FloatEvent(t4, EventCode.UPDATE, 4.f));
        
        assert( Math.abs(rs.getMin() + 3.f) < delta );
    }

    /**
     * Test of getMax method, of class RunningStatistics.
     */
    @Test
    public void testGetMax() {
        System.out.println("getMax");

        RunningStatistics rs = new RunningStatistics();
        Instant t1 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 0, 0, 0).toInstant(ZoneOffset.UTC);
        Instant t2 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 10, 0, 0).toInstant(ZoneOffset.UTC);
        Instant t3 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 20, 0, 0).toInstant(ZoneOffset.UTC);
        Instant t4 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 30, 0, 0).toInstant(ZoneOffset.UTC);

        rs.push(new FloatEvent(t1, EventCode.UPDATE, 1.f));
        rs.push(new FloatEvent(t2, EventCode.UPDATE, 21.f));
        rs.push(new FloatEvent(t3, EventCode.UPDATE, -3.f));
        rs.push(new FloatEvent(t4, EventCode.UPDATE, 4.f));

        assert( Math.abs(rs.getMax() - 21.f) < delta );
    }

    /**
     * Test of getMean method, of class RunningStatistics.
     */
    @Test
    public void testGetMean() {
        System.out.println("getMean");

        RunningStatistics rs = new RunningStatistics();
        Instant t1 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 0, 0, 0).toInstant(ZoneOffset.UTC);
        Instant t2 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 10, 0, 0).toInstant(ZoneOffset.UTC);
        Instant t3 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 20, 0, 0).toInstant(ZoneOffset.UTC);
        Instant t4 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 40, 0, 0).toInstant(ZoneOffset.UTC);

        rs.push(new FloatEvent(t1, EventCode.UPDATE, 1.f));
        rs.push(new FloatEvent(t2, EventCode.UPDATE, 2.f));
        rs.push(new FloatEvent(t3, EventCode.UPDATE, 3.f));
        rs.push(new FloatEvent(t4, EventCode.UPDATE, 4.f));

        // ( 1*10 + 2*10 + 3*20 ) / 40 = 9/4
        assert( Math.abs(rs.getMean() - 9./4.) < delta );
    }

    /**
     * Test of getSigma method, of class RunningStatistics.
     */
    @Test
    public void testGetSigma() {
        System.out.println("getSigma");

        RunningStatistics rs = new RunningStatistics();
        Instant t1 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 0, 0, 0).toInstant(ZoneOffset.UTC);
        Instant t2 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 1, 0, 0).toInstant(ZoneOffset.UTC);
        Instant t3 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 3, 0, 0).toInstant(ZoneOffset.UTC);
        Instant t4 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 5, 0, 0).toInstant(ZoneOffset.UTC);

        rs.push(new FloatEvent(t1, EventCode.UPDATE, 2.f));
        rs.push(new FloatEvent(t2, EventCode.UPDATE, 1.f));
        rs.push(new FloatEvent(t3, EventCode.UPDATE, 3.f));
        rs.push(new FloatEvent(t4, EventCode.UPDATE, 4.f));

        // mean = 2, var = sum ( w_i * (x_i - mean)^2 ) / sum ( w_i ) = [ 1*(2-2)^2 + 2*(1-2)^2 + 2*(3-2)^2 ] / 5 = 4/5. sigma = sqrt(var) = sqrt(4/5)
        assert( Math.abs(rs.getSigma() - Math.sqrt(4./5.)) < delta );
    }

    /**
     * Test of getRms method, of class RunningStatistics.
     */
    @Test
    public void testGetRms() {
        System.out.println("getRms");

        RunningStatistics rs = new RunningStatistics();
        Instant t1 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 0, 0, 0).toInstant(ZoneOffset.UTC);
        Instant t2 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 1, 0, 0).toInstant(ZoneOffset.UTC);
        Instant t3 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 3, 0, 0).toInstant(ZoneOffset.UTC);
        Instant t4 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 5, 0, 0).toInstant(ZoneOffset.UTC);

        rs.push(new FloatEvent(t1, EventCode.UPDATE, 2.f));
        rs.push(new FloatEvent(t2, EventCode.UPDATE, 1.f));
        rs.push(new FloatEvent(t3, EventCode.UPDATE, 3.f));
        rs.push(new FloatEvent(t4, EventCode.UPDATE, 4.f));

        // mean = 2, var = sum ( w_i * (x_i - mean)^2 ) / sum ( w_i ) = [ 1*(2-2)^2 + 2*(1-2)^2 + 2*(3-2)^2 ] / 5 = 4/5. RMS = sqrt(var + mean^2) = sqrt(4/5 +4)
        assert( Math.abs(rs.getRms() - Math.sqrt(4./5. + 4.)) < delta );
    }

    /**
     * Test of getDuration method, of class RunningStatistics.
     */
    @Test
    public void testGetDuration() {
        System.out.println("getDuration");

        RunningStatistics rs = new RunningStatistics();
        Instant t1 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 0, 0, 0).toInstant(ZoneOffset.UTC);
        Instant t2 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 0, 1, 0).toInstant(ZoneOffset.UTC);
        Instant t3 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 0, 3, 0).toInstant(ZoneOffset.UTC);
        Instant t4 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 0, 5, 0).toInstant(ZoneOffset.UTC);

        rs.push(new FloatEvent(t1, EventCode.UPDATE, 2.f));
        rs.push(new FloatEvent(t2, EventCode.NETWORK_DISCONNECTION, 0));
        rs.push(new FloatEvent(t3, EventCode.UPDATE, 3.f));
        rs.push(new FloatEvent(t4, EventCode.UPDATE, 4.f));

        assert( Math.abs(rs.getDuration() - 3.) < delta );
    }

    /**
     * Test of getIntegration method, of class RunningStatistics.
     */
    @Test
    public void testGetIntegration() {
        System.out.println("getIntegration");

        RunningStatistics rs = new RunningStatistics();
        Instant t1 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 0, 0, 0).toInstant(ZoneOffset.UTC);
        Instant t2 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 0, 1, 0).toInstant(ZoneOffset.UTC);
        Instant t3 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 0, 3, 0).toInstant(ZoneOffset.UTC);
        Instant t4 = LocalDateTime.of(2017, Month.JANUARY, 1, 0, 0, 5, 0).toInstant(ZoneOffset.UTC);

        rs.push(new FloatEvent(t1, EventCode.UPDATE, 2.f));
        rs.push(new FloatEvent(t2, EventCode.UPDATE, 1.f));
        rs.push(new FloatEvent(t3, EventCode.UPDATE, 3.f));
        rs.push(new FloatEvent(t4, EventCode.UPDATE, 4.f));

        // 2*1 + 1*2 + 3*2 = 10
        
        assert( Math.abs(rs.getIntegration() - 10.) < delta );
    }

}
