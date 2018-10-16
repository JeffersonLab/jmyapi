package org.jlab.mya;

/**
 * The parameters required to query for events. The PV Metadata is an essential
 * query parameter because it indicates which host to query and for which
 * database ID.
 *
 * @author slominskir
 */
public abstract class QueryParams {

    private final Metadata metadata;
    private final boolean updatesOnly;

    /**
     * Create a new QueryParams for both update and informational events.
     *
     * @param metadata The PV metadata
     */
    public QueryParams(Metadata metadata) {
        this(metadata, false);
    }

    /**
     * Create a new QueryParams for update and optionally informational events.
     *
     * @param metadata The PV metadata
     * @param updatesOnly true for updates events only, false otherwise
     */
    public QueryParams(Metadata metadata, boolean updatesOnly) {
        this.metadata = metadata;
        this.updatesOnly = updatesOnly;
    }

    /**
     * Return the Metadata "PV Query Key".
     *
     * @return The Metadata
     */
    public Metadata getMetadata() {
        return metadata;
    }

    /**
     * Return whether all events or only updates are queried.
     *
     * @return true for only update events, false for all events.
     */
    public boolean isUpdatesOnly() {
        return updatesOnly;
    }
}