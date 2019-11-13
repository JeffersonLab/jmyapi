package org.jlab.mya.stream.wrapped;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import org.jlab.mya.EventCode;
import org.jlab.mya.event.FloatEvent;

/**
 * This is intended to be used as the "bucket" in a modified implementation of the largest triangle three bucket (LTTB) algorithm
 * described in "Downsampling Time Series for Visual Representation" (Steinarsson, 2013).  In addition to determining the
 * LTTB point for this bucket, it also collects any non-update events, the minimum event by value, and the maximum event
 * by value.
 * 
 * @author apcarp
 */
public class FloatEventBucket {
    private List<FloatEvent> events;
    private final List<FloatEvent> output = new ArrayList<>(); // Non-update events will be added here
    private FloatEvent min = null;
    private FloatEvent max = null;
    private FloatEvent lttb = null;
    private double lttbArea = 0;
    
    /**
     * Instantiate a FloatEventBucket object
     * @param events A list of FloatEvents to be represented in this bucket
     */
    public FloatEventBucket (List<FloatEvent> events) {
        if (events == null || events.isEmpty()) {
            throw new IllegalArgumentException("FloatEventBucket requires non-null, non-empty list of events.");
        }
        this.events = events;
    }
    
    /**
     * This iterates over events in bucket finding the point which creates the largest  triangular area with the two provided points.
     * This also finds and saves any other points that will be included in the downsampled output.
     * @param e1 The first point to be used in the LTTB triangle area calculation (typically the LTTB point from the preceding bucket)
     * @param e3 The last point to be used in the LTTB triangle area calculation (typically the LTTB point from the following bucket)
     * @return Returns the found LTTB point.  Meant to be used in downSample calls on following bins.
     */
    public FloatEvent downSample (FloatEvent e1, FloatEvent e3) {
        if ( e1 == null || e3 == null ) {
            throw new IllegalArgumentException("Two non-null events required");
        }

        // Search the list for the LTTB point and any other points of interest.  Specifically, we care about non-update events,
        // bucket min, and bucket max.
        double area;
        
        // Since we are saving non-update events, we should also include the adjacent points to accurately display the
        // disconnect.  If non-update event, look back for last point.  If update, look back to see if last was non-update.
        FloatEvent prev = null;

        for(FloatEvent e : events) {

            // Non-update events - filter these out first since they don't have meaningful values
            if ( e.getCode() != EventCode.UPDATE ) {
                if (prev != null ) {
                    output.add(prev);
                }
                output.add(e);
                prev = e;
                continue;
            } else if ( prev != null && prev.getCode() != EventCode.UPDATE ) {
                output.add(e);
                // no continue here since this point may also be the min/max/lttb.  We'll end up with fewer
                // points on average since duplicate hits get removed.
            }
            
            // LTTB check
            area = calculateTriangleArea(e1, e, e3);
            if (area > lttbArea) {
                lttbArea = area;
                lttb = e;
            }
            
            // min / max
            if ( min == null || e.getValue() < min.getValue() ) {
                min = e;
            }
            if ( max == null || e.getValue() > max.getValue() ) {
                max = e;
            }
            
            prev = e;
        }
        
        // If the bucket contained nothing but non-update events, you get the previous LTTB point back.  This seems better
        // than setting the point to another arbitrary point.
        if ( lttb == null ) {
            return e1;
        } else {
            return lttb;
        }
    }

    /**
     * Return the downSampled representation of this bucket.
     * @return Downsampled set of FloatEvents representing this FloatEventBucket
     */
    public SortedSet<FloatEvent> getDownSampledOutput() {
        SortedSet<FloatEvent> sampledOutput = new TreeSet<>();
        if ( lttb != null ) {
            sampledOutput.add(lttb);
        }
        if ( min != null ) {
            sampledOutput.add(min);            
        }
        if ( max != null ) {
            sampledOutput.add(max);
        }
        sampledOutput.addAll(output);
        return sampledOutput;
    }
    
    /**
     * This calculates the area of the triangle formed by three FloatEvents
     *
     * @param e1 event 1 of the triangle
     * @param e2 event 2 of the triangle
     * @param e3 event 3 of the triangle
     * @return The triangle area
     */
    protected static double calculateTriangleArea(FloatEvent e1, FloatEvent e2, FloatEvent e3) {

        // define the first point as the time origin, then normalize the other points
        double x1 = 0.0;
        double x2 = e2.getTimestampAsSeconds() - e1.getTimestampAsSeconds();
        double x3 = e3.getTimestampAsSeconds() - e1.getTimestampAsSeconds();

        // If e3 is a non-update event, then just use the value of e1.  Keeping track of a running average (or something)
        // may be a smarter choice, but this whole thing is complicated enough as it is.
        //
        // We assume e1 is always a non-update event.  Seems reasonable since we iterate over the initial points until
        // we find an UPDATE event which becomes the first lttb/e1 point, and the downsample always passes forward the
        // last good lttb point (e1) if no UPDATE events were in the bucket.
        double y3;
        if (e3.getCode() != EventCode.UPDATE) {
            y3 = e1.getValue();
        } else {
            y3 = e3.getValue();
        }

        // The values are floats that get cast to doubles, so I'm not too worried about adding extra rounding errors due to difference
        // in scale.
        // Formula for trianlge in 2d space is x1*(y2-y3) - x2*(y3-y1) - x3*(y1-y2).  But we may have to be tricky with y3 here.
        return 0.5 * Math.abs(x1 * (e2.getValue() - y3) + x2 * (y3 - e1.getValue()) + x3 * (e1.getValue() - e2.getValue()) );
    }
}
