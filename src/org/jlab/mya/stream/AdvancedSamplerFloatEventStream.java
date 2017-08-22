package org.jlab.mya.stream;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import org.jlab.mya.EventCode;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.params.AdvancedSamplerParams;

/**
 * EventStream for the Improved sampler. This stream reads the full dataset from the database and
 * returns a subset (performs application layer filtering).
 *
 * @author ryans
 */
public class AdvancedSamplerFloatEventStream extends FloatEventStream {

    private final AdvancedSamplerParams samplerParams;
    private final long binSize;
    private final Queue<FloatEvent> queue = new PriorityQueue<>();
    private boolean hasFirst = false;
    private FloatEvent lastLTTB = null;
    private long binBoundary = 0;
    private long pointsProcessed = 0;
    private final List<FloatEvent> events = new ArrayList<>();

    // We use to points so we can look ahead and handle the last point similar to the first and process the last "real" bucket
    private FloatEvent prev = null;
    private FloatEvent curr = null;
        
    
    /**
     * Create a new ImprovedSamplerFloatStream.
     * 
     * @param params The query parameters
     * @param con The database connection
     * @param stmt The database statement
     * @param rs The database result set
     */
    public AdvancedSamplerFloatEventStream(AdvancedSamplerParams params, Connection con,
            PreparedStatement stmt, ResultSet rs) {
        super(params, con, stmt, rs);
        this.samplerParams = params;
        
        // 10 years of nanos starts to approach the range of overflow concerns.  Millis will be good enough to split on.
        long bins = params.getNumBins();
        
        // The user request numBins.  The first and last point/bin get consumned by the first and last point so the -2s. 
        // Then, we may not have enough points to fill all bins exactly, so use ceiling to ensure that bins are the proper size
        // for numBins - 2 to hold count - 2 without adding extra bins.
        binSize =  (long) (Math.ceil( ( (double) params.getCount() - 2) / (params.getNumBins() - 2)));
        
        boolean first = false;
        boolean last = false;        
    }

    @Override
    public FloatEvent read() throws IOException {
        
        // If the queue is empty, process some more of the stream which should add data to the queue
        if ( queue.peek() == null ) {
            processStream();
        }
        // Returns either the next FloatEvent, or null.  If null, then there is no more data and the requestor should call close.
        return queue.poll();
    }

    private void processStream() throws IOException{
        
        // The first real point should be included no matter what and should be used as the first LTTB point.
        // Queue up any non-update points before the first real update.
        if ( ! hasFirst ) {
            FloatEvent first;

            // Keep reading events and putting them on the queue until you find the first "update" event
            while( (first = super.read()) != null && (! first.getCode().equals(EventCode.UPDATE)) ) {
                queue.add(first);
                pointsProcessed++;
            }
            
            if (first != null) {
                hasFirst = true;
                lastLTTB = first;
                queue.add(lastLTTB);
                pointsProcessed++;
                
                // Could be enough non-update events to have consumed the first bucket's worth of points.  Set the binBoundary
                // to the next boundary after the number of processed points
                binBoundary = (pointsProcessed / binSize + 1) * binSize;
            }
            return;
        }

        prev = curr;
        while( ( curr = super.read()) != null ) {
            if ( prev != null ) {
                events.add(prev);
                pointsProcessed++;
                
                if (pointsProcessed  == binBoundary) {
                    binBoundary = binBoundary + binSize;
                    FloatEventBucket feb = new FloatEventBucket(events);
                    lastLTTB = feb.downSample(lastLTTB, curr);
                    queue.addAll(feb.getDownSampledOutput());
                    events.clear();
                    return;  // We only want to queue up one bucket's worth of downsampled points at a time
                }
            }
            prev = curr;
        }
        
        if ( prev != null ) {
            // We're here, so no more points to be read from the resultSet.  Create a bucket with the event list, downsample and
            // write it to the queue, then add the last point to the queue (prev should not have been added to event list yet).  Clear 
            // the listand when a subsequent read/procesStream happens, processStream will not queue anything else up and the 
            // queue will return null.
            if ( ! events.isEmpty()) {
                FloatEventBucket feb = new FloatEventBucket(events);
                lastLTTB = feb.downSample(lastLTTB, prev);
                queue.addAll(feb.getDownSampledOutput());
                events.clear();
            }
            queue.add(prev);
        } else {
            // This must be a read request after we've exhausted our resultSet, so don't do anything.  The queue should return
            // null, and the client should close the connection.
        }
    }
}
