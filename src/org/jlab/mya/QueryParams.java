package org.jlab.mya;

/**
 * The parameters required to query for events. The PV Metadata is an essential query parameter
 * because it indicates which host to query and for which database ID.
 *
 * @author slominskir
 */
public abstract class QueryParams {

    private final Metadata metadata;

    /**
     * Create a new QueryParams.
     *
     * @param metadata The PV metadata
     */
    public QueryParams(Metadata metadata) {
        this.metadata = metadata;
    }

    /**
     * Return the Metadata "PV Query Key".
     *
     * @return The Metadata
     */
    public Metadata getMetadata() {
        return metadata;
    }
}
