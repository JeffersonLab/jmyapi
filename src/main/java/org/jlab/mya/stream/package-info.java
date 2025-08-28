/**
 * Provides classes for wrapping streams. <b>Example</b>
 *
 * <pre>
 *         DataNexus nexus = new OnDemandNexus("ops");
 *
 *         String pv = "R123PMES";
 *         Instant begin = TimeUtil.toLocalDT("2017-01-01T00:00:00.123456");
 *         Instant end = TimeUtil.toLocalDT("2017-01-01T00:01:00.123456");
 *
 *         Metadata&lt;FloatEvent&gt; metadata = nexus.findMetadata(pv, FloatEvent.class);
 *
 *         long count = nexus.count(metadata, begin, end);
 *
 *         FloatEvent priorPoint = nexus.findEvent(metadata, begin);
 *
 *         long bins = 10;
 *         short[] eventStatsMap = new short[]{RunningStatistics.INTEGRATION};
 *
 *         try (
 *                 // Get stream of FloatEvents
 *                 final EventStream&lt;FloatEvent&gt; stream = nexus.openEventStream(metadata, begin, end);
 *
 *                 // Ensure points exist on the interval boundaries
 *                 final BoundaryAwareStream&lt;FloatEvent&gt; boundaryStream = new BoundaryAwareStream&lt;&gt;(stream, begin, end, priorPoint, false, FloatEvent.class);
 *
 *                 // Analyze the stream and integrate
 *                 final FloatAnalysisStream analysisStream = new FloatAnalysisStream(boundaryStream, eventStatsMap);
 *
 *                 // Downsample the results for easy graphical display (such as on a web browser)
 *                 final FloatGraphicalSampleStream&lt;AnalyzedFloatEvent&gt; sampleStream = new FloatGraphicalSampleStream&lt;&gt;(analysisStream, bins, count, AnalyzedFloatEvent.class);
 *             ) {
 *
 *             AnalyzedFloatEvent event;
 *
 *             while ((event = sampleStream.read()) != null) {
 *                 System.out.println(event.toString(2) + ", integration: " + event.getEventStats()[0]);
 *             }
 *
 *             RunningStatistics stats = analysisStream.getLatestStats();
 *
 *             System.out.println("Max: " + stats.getMax());
 *             System.out.println("Min: " + stats.getMin());
 *             System.out.println("Mean: " + stats.getMean());
 *         }
 *             </pre>
 */
package org.jlab.mya.stream;
