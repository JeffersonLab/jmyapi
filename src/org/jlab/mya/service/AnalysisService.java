/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.jlab.mya.service;

import java.io.IOException;
import java.sql.SQLException;
import org.jlab.mya.DataNexus;
import org.jlab.mya.TimeUtil;
import org.jlab.mya.analysis.RunningStatistics;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.params.IntervalQueryParams;
import org.jlab.mya.params.PointQueryParams;
import org.jlab.mya.stream.FloatEventStream;

/**
 *
 * @author adamc
 */
public class AnalysisService extends IntervalService {
    
    public AnalysisService(DataNexus nexus) {
        super(nexus);
    }
    
    public RunningStatistics calculateRunningStatistics(IntervalQueryParams params) throws SQLException, IOException {
        
        RunningStatistics rs = new RunningStatistics();
        PointQueryParams pqParams = new PointQueryParams(params.getMetadata(), params.getBegin());
        PointService pqService = new PointService(this.nexus);
        try ( FloatEventStream stream = this.openFloatStream(params) ) {
            
            // Get the first event from before the interval, then map it to an event that matches the start of the query.  This will
            // make the duration, and other statistics correct for the query.  If null, then there was no point in the archiver from before
            // the interval query and the interval query should catch the first point if it existed inside the interval
            FloatEvent event = pqService.findFloatEvent(pqParams);
            if ( event != null ) {
                rs.push(new FloatEvent(TimeUtil.toMyaTimestamp(params.getBegin()), event.getCode(), event.getValue()));
            }
            
            FloatEvent last = event;
            while ( (event  = stream.read()) != null ) {
                last = event;
                // Add each event - the push method updates the statistics based on the new data
                rs.push(event);
            }
            
            // Create an event that represents the value of the signal at the end of the query
            if ( last != null ) {
                rs.push(new FloatEvent(TimeUtil.toMyaTimestamp(params.getEnd()), last.getCode(), last.getValue()));
            }
        }
        
        return rs;
    }
}
