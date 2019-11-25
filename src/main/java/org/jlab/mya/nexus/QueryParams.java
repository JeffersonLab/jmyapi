package org.jlab.mya.nexus;

import org.jlab.mya.event.Event;
import org.jlab.mya.Metadata;

/**
 * The parameters required to query for events. The PV Metadata is an essential
 * query parameter because it indicates which host to query and for which
 * database ID.
 *
 * @author slominskir
 */
abstract class QueryParams<T extends Event> {

    private final Metadata<T> metadata;
    private final boolean updatesOnly;

    /**
     * Create a new QueryParams for both update and informational events.
     *
     * @param metadata The PV metadata
     */
    public QueryParams(Metadata<T> metadata) {
        this(metadata, false);
    }

    /**
     * Create a new QueryParams for update and optionally informational events.
     *
     * @param metadata The PV metadata
     * @param updatesOnly true for updates events only, false otherwise
     */
    protected QueryParams(Metadata<T> metadata, boolean updatesOnly) {
        this.metadata = metadata;
        this.updatesOnly = updatesOnly;
    }

    /**
     * Return the Metadata "PV Query Key".
     *
     * @return The Metadata
     */
    public Metadata<T> getMetadata() {
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
