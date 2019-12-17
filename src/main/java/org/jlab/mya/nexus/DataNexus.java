package org.jlab.mya.nexus;

import org.jlab.mya.*;
import org.jlab.mya.event.Event;
import org.jlab.mya.event.FloatEvent;
import org.jlab.mya.stream.EventStream;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/**
 * Manages the possibly multiple data sources required to service requests to a
 * cluster of Mya hosts, which make up a deployment.
 *
 * A configuration file (Java properties) is required to be in the class path:
 * "deployments.properties". If this file is not found this class will throw an
 * ExceptionInInitializerError upon being loaded by the class loader.
 *
 * @author slominskir
 */
@SuppressWarnings("DuplicatedCode")
public abstract class DataNexus {

    private IntervalService intervalService = new IntervalService(this);
    private PointService pointService = new PointService(this);
    private SourceSamplingService sourceSampleService = new SourceSamplingService(this);

    /**
     * The application's deployments configuration properties.
     */
    static final Properties DEPLOYMENTS_PROPERTIES = new Properties();

    static {
        // Load MYA Deployment Config
        try (InputStream is
                = DataNexus.class.getClassLoader().getResourceAsStream(
                "deployments.properties")) {
            if (is == null) {
                throw new IOException(
                        "File Not Found; Configuration File: deployments.properties");
            }

            DEPLOYMENTS_PROPERTIES.load(is);
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final String deployment;

    /**
     * Create a new DataNexus for the given deployment.
     *
     * @param deployment The Mya deployment
     */
    protected DataNexus(String deployment) {
        if (DataNexus.getDeploymentNames().contains(deployment)) {
            this.deployment = deployment;
        } else {
            throw new IllegalArgumentException("Deployment definition not found: " + deployment);
        }
    }

    /**
     * Return the Mya deployment.
     *
     * @return The deployment
     */
    public String getDeployment() {
        return deployment;
    }
    
    /**
     * Return the set of valid deployment names.
     * @return The Set of valid deployment names.
     */
    public static Set<String> getDeploymentNames() {
        Set<String> names = new TreeSet<>();
        for(Object o : DEPLOYMENTS_PROPERTIES.keySet()) {
            String prop = (String) o;
            if (prop.matches("\\w+.master.host")) {
                names.add(prop.split("\\.")[0]);
            }
        }
        return names;
    }
    
    /**
     * Return the host name of the master host for the deployment associated
     * with this DataNexus.
     *
     * @return The master host name
     */
    public String getMasterHostName() {
        return DEPLOYMENTS_PROPERTIES.getProperty(deployment + ".master.host");
    }

    /**
     * Return a connection to the specified host.
     *
     * Note: clever implementations may be caching / pooling connections and may
     * actually return a wrapped connection to you.
     *
     * @param host The Mya host name
     * @return A MySQL database connection (or wrapped connection)
     * @throws SQLException If unable to obtain a connection
     */
    abstract Connection getConnection(String host) throws SQLException;

    /**
     * Paginated wildcard query for a list of PVs (Channels) like the specified name.  Standard SQL wildcards (% and _)
     * are supported as an SQL like query is performed.   For example to query for a name containing "R123" you
     * would use "%R123%".
     *
     * @param q The wildcard search query
     * @param limit max results
     * @param offset page though query starting index (start at zero)
     * @return The paginated list of channel metadata like name
     * @throws SQLException If unable to query the database
     */
    public List<Metadata> findChannel(String q, long limit, long offset) throws SQLException {
        return intervalService.findChannel(q, limit, offset);
    }

    /**
     * Query for PV metadata given PV name.
     *
     * @param name The PV name
     * @return PV metadata
     * @throws SQLException If unable to query the database
     */
    public Metadata findMetadata(String name) throws SQLException {
        return intervalService.findMetadata(name);
    }

    /**
     * Query for PV metadata given PV name and specified type.
     *
     * @param name The PV name
     * @param type The type
     * @param <T> The Event type
     * @return PV metadata
     * @throws SQLException If unable to query the database
     * @throws ClassCastException If the PV has an incompatible type
     */
    public <T extends Event> Metadata<T> findMetadata(String name, Class<T> type) throws SQLException, ClassCastException {
        return intervalService.findMetadata(name, type);
    }

    /**
     * Query for PV extra info given PV metadata, and optionally type, begin bounds, and end bounds.  ExtraInfo types
     * include "enum_labels" and "notes".
     *
     * <p>
     * <b>Note:</b> Unlike Metadata, ExtraInfo is versioned and changes over time.
     * </p>
     *
     * @param metadata The PV metadata
     * @param type The type of info to query, null for all
     * @param begin limit results to just the first one before begin plus all afterwards or null for all
     * @param end limit results to just those before end or null for all
     * @return The extra info
     * @throws SQLException If unable to query the database
     */
    public List<ExtraInfo> findExtraInfo(Metadata metadata, String type, Instant begin, Instant end) throws SQLException {
        return intervalService.findExtraInfo(metadata, type, begin, end);
    }

    /**
     * Open a stream to events associated with the specified
     * IntervalQueryParams. This method returns a stream with the specified type.
     *
     * Generally you'll want to use try-with-resources around a call to this
     * method to ensure you close the stream properly.
     *
     * <p>
     * Implementation Note: Some values are primitives so a generic getValue() method returning a generic would not
     * work as it would result in additional autoboxing performance costs.
     * </p>
     *
     * @param metadata The metadata
     * @param begin The begin timestamp (inclusive)
     * @param end The end timestamp (exclusive)
     * @param strategy The fetch strategy
     * @param updatesOnly true if only update events should be included
     * @param <T> The Event type
     * @return a stream
     * @throws SQLException If unable to query the database
     */
    public <T extends Event> EventStream<T> openEventStream(Metadata<T> metadata, Instant begin, Instant end, IntervalQueryFetchStrategy strategy, boolean updatesOnly) throws SQLException {
        return intervalService.openEventStream(new IntervalQueryParams<>(metadata, updatesOnly, strategy, begin, end));
    }

    /**
     * Open a stream to events associated with the specified
     * parameters. This method returns a stream with the specified type.
     *
     * Generally you'll want to use try-with-resources around a call to this
     * method to ensure you close the stream properly.
     *
     * <p>
     * Implementation Note: Some values are primitives so a generic getValue() method returning a generic would not
     * work as it would result in additional autoboxing performance costs.
     * </p>
     *
     * @param metadata The metadata
     * @param begin The begin timestamp (inclusive)
     * @param end The end timestamp (exclusive)
     * @param <T> The Event type
     * @return a stream
     * @throws SQLException If unable to query the database
     */
    public <T extends Event> EventStream<T> openEventStream(Metadata<T> metadata, Instant begin, Instant end) throws SQLException {
        return intervalService.openEventStream(new IntervalQueryParams<>(metadata, begin, end));
    }

    /**
     * Count the number of events between begin and end instants potentially restricted to update events only.
     *
     * @param metadata The metadata
     * @param begin The begin timestamp (inclusive)
     * @param end The end timestamp (exclusive)
     * @param updatesOnly true if only update events should be counted
     * @return The number of events
     * @throws SQLException If unable to query the database
     */
    @SuppressWarnings("unchecked")
    public long count(Metadata metadata, Instant begin, Instant end, boolean updatesOnly) throws SQLException {
        return intervalService.count(new IntervalQueryParams(metadata, updatesOnly, IntervalQueryFetchStrategy.STREAM, begin, end));
    }

    /**
     * Count the number of events between begin and end instants including all event types.
     *
     * @param metadata The metadata
     * @param begin The begin timestamp (inclusive)
     * @param end The end timestamp (exclusive)
     * @return The number of events
     * @throws SQLException If unable to query the database
     */
    @SuppressWarnings("unchecked")
    public long count(Metadata metadata, Instant begin, Instant end) throws SQLException {
        return intervalService.count(new IntervalQueryParams(metadata, begin, end));
    }

    /**
     * Find the first event occurring at a time optionally less than and optionally equal to the timestamp
     * provided and optionally including only update events.
     *
     * @param metadata The PV metadata
     * @param timestamp The timestamp
     * @param lessThan true if an event less than the point-in-time, false for
     * an event greater than the point-in-time.
     * @param orEqual true if the point exactly at the given timestamp is
     * returned, false if the timestamp is exclusive
     * @param updatesOnly true to include updates only, false for all event types
     * @param <T> The event type
     * @return An Event or null if none found
     * @throws SQLException If unable to query the database
     */
    public <T extends Event> T findEvent(Metadata<T> metadata, Instant timestamp, boolean lessThan, boolean orEqual, boolean updatesOnly) throws SQLException {
        return pointService.findEvent(new PointQueryParams<>(metadata, updatesOnly, timestamp, lessThan, orEqual));
    }

    /**
     * Find the first event occurring at a time less than or equal to the timestamp
     * provided and including all event types.
     *
     * @param metadata The PV metadata
     * @param timestamp The timestamp
     * @param <T> The event type
     * @return An Event or null if none found
     * @throws SQLException If unable to query the database
     */
    public <T extends Event> T findEvent(Metadata<T> metadata, Instant timestamp) throws SQLException {
        return pointService.findEvent(new PointQueryParams<>(metadata, timestamp));
    }

    /**
     * Open a stream to float events associated with the specified
     * IntervalQueryParams and sampled using the myGet sampling algorithm.
     *
     * The myget algorithm is the what you get with MYA 'myget -l'. A stored procedure is used,
     * and does not always provide
     * a consistent spacing of data and does not weigh the data. This approach is generally comparatively fast
     * vs other sampling methods. It appears the procedure
     * simply divides the interval into sub-intervals based on the number of
     * requested points and then queries for the first event in each
     * sub-interval. If no event is found in the sub-interval then no point is
     * provided for that sub-interval meaning that the user may get less points
     * then the limit. Sub-intervals are spaced by date.
     *
     * Generally you'll want to use try-with-resources around a call to this
     * method to ensure you close the stream properly.
     *
     * Note: Implemented using a database stored procedure.
     *
     * @param metadata The metadata
     * @param begin The begin timestamp (inclusive)
     * @param end The end timestamp (exclusive)
     * @param limit The max number of bins
     * @return A new EventStream
     * @throws SQLException If unable to query the database
     */
    public EventStream<FloatEvent> openMyGetSampleStream(Metadata<FloatEvent> metadata, Instant begin, Instant end, long limit) throws
            SQLException {
        return sourceSampleService.openMyGetSampleFloatStream(metadata, begin, end, limit);
    }

    /**
     * Open a stream to float events associated with the specified
     * IntervalQueryParams and sampled using the mySampler algorithm.
     *
     * The mySampler algorithm is what you get with MYA 'mySampler'. Each sample is
     * obtained from a separate query. Bins are based on date.
     *
     * Generally you'll want to use try-with-resources around a call to this
     * method to ensure you close the stream properly.
     *
     * @param metadata The metadata
     * @param begin The begin timestamp (inclusive)
     * @param stepMilliseconds The milliseconds between bins
     * @param sampleCount The number of samples
     * @return A new EventStream
     * @throws SQLException If unable to query the database
     */
    public EventStream<FloatEvent> openMySamplerStream(Metadata<FloatEvent> metadata, Instant begin, long stepMilliseconds, long sampleCount) throws
            SQLException {
        return sourceSampleService.openMySamplerFloatStream(metadata, begin, stepMilliseconds, sampleCount);
    }

    /**
     * The fetch strategy for obtaining records from the database.  See the JUnit tests which show STREAM is generally
     * the correct choice and conditionally choosing between them based on record count likely isn't worth your time.
     */
    public enum IntervalQueryFetchStrategy {
        /**
         * Fetch all records into memory at once.  Not recommended for large data sets as you may encounter an
         * OutOfMemoryError.   May have slightly faster throughput for small datasets.  High latency for large
         * datasets (must wait for all records to transferred before ResultSet can be iterated).
         */
        ALL,
        /**
         * Fetch records in chunks (batches).  This is implemented via a MySQL server-side database cache.  The database
         * creates a temporary table and then provides a database cursor for
         * the client to request batches of records (currently 4096 at a time).  This turns out to be generally the
         * least desirable strategy inheriting the worst of the other approaches: you are penalized with the latency
         * penalty of waiting for a database server-side temporary table to be filled entirely, plus memory pressure is
         * simply transferred from the client to the database server, which generally is a worse place to shift it
         * scalability-wise.  Also the database must hold the cursor resource open for an unspecified amount of time
         * while the client does the processing.
         */
        CHUNK,
        /**
         * Default jmyapi fetch strategy in which records are streamed one at a time to use the least amount of memory.
         * This approach has the best performance with large datasets as record processing can begin as soon as the
         * first record arrives (low latency) and the risk of an OutOfMemoryError is low.  In practice some
         * buffering may occur via the MySQL database driver to minimize round-trip overhead.
         */
        STREAM
    }
}
