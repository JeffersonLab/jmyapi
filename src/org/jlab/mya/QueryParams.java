package org.jlab.mya;

/**
 * The parameters required to query for events.
 *
 * @author slominskir
 */
public class QueryParams {

    private final Metadata metadata;

    /**
     * Create a new QueryParams.
     *
     * @param metadata The PV metadata
     */
    public QueryParams(Metadata metadata) {
        this.metadata = metadata;
    }

    public Metadata getMetadata() {
        return metadata;
    }
}
