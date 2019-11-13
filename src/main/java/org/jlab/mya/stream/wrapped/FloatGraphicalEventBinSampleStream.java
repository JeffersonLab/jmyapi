package org.jlab.mya.stream.wrapped;

import org.jlab.mya.EventCode;
import org.jlab.mya.EventStream;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.params.GraphicalEventBinSamplerParams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Wraps a FloatEventStream and provides FloatEvents that are down-sampled as they stream by using an algorithm
 * which attempts to maintain graphical fidelity of the data.
 * <p>
 * This stream reads the full dataset from the database and returns a subset (performs application layer filtering).
 * </p>
 * <p>
 * The intention is to provide a sampling of the original data that maintains
 * critical data points such as first/last values or non-update (e.g.,
 * disconnect) events, and maintains graphical fidelity.
 * </p>
 * <p>
 * Currently this is done using a custom streaming variant of the Largest
 * Triangle Three Bucket (LTTB) algorithm presented by Sveinn Steinarsson in his
 * Master's Thesis "Downsampling Time Series for Visual Representation" (Steinarsson, 2013). An
 * oversimplified description of this approach is to segment the raw data into
 * buckets (i.e., bins), then keep a point from each bin with the goal of
 * creating the largest triangular area. Other points are also kept to maintain
 * graphical fidelty. See documentation on FloatEventBucket for more details on
 * the downsampling algorithm.
 * </p>
 * <p>
 * In addition to determining the LTTB point for this
 * bucket, it also collects and non-update events, the minimum event by
 * value, and the maximum event by value. This is an online algorithm and
 * does not persist data.
 * </p>
 * <p>
 * Note: The the number of bins may vary from the specified number.  The exact
 * number of bins is ceil( (count - 2) / ceil((count - 2) / (numBins - 2) + 2)) plus
 * the first and last points.  In other words, we strip off the first and last points
 * then divide the remaining events into bins of size k where k is the smallest size that
 * requires at most numBins-2 to contain the entire event set.
 * </p>
 * <p>
 * Note: This algorithm promises not to create new FloatEvents.  Instead events are simply filtered out.  This
 * means you can pass in FloatEvent subclasses such as AnalyzedFloatEvents and they'll come out untouched.
 * </p>
 * @author apcarp
 * @author slominskir
 */
public class FloatGraphicalEventBinSampleStream extends WrappedEventStreamAdaptor<FloatEvent, FloatEvent> {

    private final GraphicalEventBinSamplerParams samplerParams;
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
     * Create a new FloatGraphicalEventBinSampleStream by wrapping a FloatEventStream.
     *
     * @param stream The FloatEventStream to wrap
     * @param params The GraphiaclEventBinSampleParams
     */
    public FloatGraphicalEventBinSampleStream(EventStream<FloatEvent> stream, GraphicalEventBinSamplerParams params) {
        super(stream);

        this.samplerParams = params;

        // 10 years of nanos starts to approach the range of overflow concerns.  Millis will be good enough to split on.
        //
        // The user request numBins.  The first and last point/bin get consumned by the first and last point so the -2s.
        // Then, we may not have enough points to fill all bins exactly, so use ceiling to ensure that bins are the proper size
        // for numBins - 2 to hold count - 2 without adding extra bins.
        binSize = (long) (Math.ceil(((double) params.getCount() - 2) / (params.getNumBins() - 2)));
    }

    /**
     * Read the next event from the stream. Generally you'll want to iterate
     * over the stream using a while loop.
     *
     * @return The next event or null if End-Of-Stream reached
     * @throws IOException If unable to read the next event
     */
    @Override
    public FloatEvent read() throws IOException {
        // If the queue is empty, process some more of the stream which should add data to the queue
        if (queue.peek() == null) {
            processStream();
        }
        // Returns either the next FloatEvent, or null.  If null, then there is no more data and the requestor should call close.
        return queue.poll();
    }

    /**
     * This method processes the "raw" FloatEventStream and supplies write
     * events according to a modified LTTB approach out to an internal queue.
     *
     * @throws IOException
     */
    private void processStream() throws IOException {

        // The first real point should be included no matter what and should be used as the first LTTB point.
        // Queue up any non-update points before the first real update.
        if (!hasFirst) {
            FloatEvent first;

            // Keep reading events and putting them on the queue until you find the first "update" event
            while ((first = wrapped.read()) != null && (!first.getCode().equals(EventCode.UPDATE))) {
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
        while ((curr = wrapped.read()) != null) {
            if (prev != null) {
                events.add(prev);
                pointsProcessed++;

                if (pointsProcessed == binBoundary) {
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

        if (prev != null) {
            // We're here, so no more points to be read from the resultSet.  Create a bucket with the event list, downsample and
            // write it to the queue, then add the last point to the queue (prev should not have been added to event list yet).  Clear
            // the listand when a subsequent read/procesStream happens, processStream will not queue anything else up and the
            // queue will return null.
            if (!events.isEmpty()) {
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
