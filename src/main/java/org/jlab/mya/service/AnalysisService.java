package org.jlab.mya.service;

import java.io.IOException;
import java.sql.SQLException;

import org.jlab.mya.DataNexus;
import org.jlab.mya.TimeUtil;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.params.IntervalQueryParams;
import org.jlab.mya.params.PointQueryParams;
import org.jlab.mya.stream.FloatEventStream;
import org.jlab.mya.analysis.RunningStatistics;

/**
 * Provides access to summary statistics about a MYA channel data.
 * @author apcarp
 */
public class AnalysisService extends IntervalService {
    
    /**
     *  Create a new service with the provided DataNexus
     * @param nexus The DataNexus
     */
    public AnalysisService(DataNexus nexus) {
        super(nexus);
    }
    
    /**
     * This calculates summary statistics based on the provided IntervalQueryParams object.  The statistics are accessed via
     * the return RunningStatistics object.
     * @param params Specify the details of the query
     * @return A RunningStatistics object containing the summary statistic information of the query.
     * @throws SQLException If unable to query the database
     * @throws IOException If unable to read from the stream
     */
    public RunningStatistics calculateRunningStatistics(IntervalQueryParams params) throws SQLException, IOException {
        
        RunningStatistics rs = new RunningStatistics();
        PointQueryParams pqParams = new PointQueryParams(params.getMetadata(), params.getBegin());
        PointService pqService = new PointService(this.nexus);

        // Get the first event from before the interval, then map it to an event that matches the start of the query.  This will
        // make the duration, and other statistics correct for the query.  If null, then there was no point in the archiver from before
        // the interval query and the interval query should catch the first point if it existed inside the interval
        FloatEvent event = pqService.findFloatEvent(pqParams);
        if ( event != null ) {
            rs.push(new FloatEvent(TimeUtil.toMyaTimestamp(params.getBegin()), event.getCode(), event.getValue()));
        }

        try ( FloatEventStream stream = this.openFloatStream(params) ) {
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
