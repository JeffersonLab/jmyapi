package org.jlab.mya;

import java.time.Instant;

/**
 * Provides additional information about an EPICS PV, which unlike standard
 * Metadata, is versioned (may change over time).
 *
 * @author ryans
 */
public final class ExtraInfo {
    private final Metadata metadata;
    private final Instant timestamp;
    private final String type;
    private final String value;

    public ExtraInfo(Metadata metadata, String type, Instant timestamp, String value) {
        this.metadata = metadata;
        this.type = type;
        this.timestamp = timestamp;
        this.value = value;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public String getType() {
        return type;
    }

    public Instant getTimestamp() {
        return timestamp;
    }
    
    public String getValue() {
        return value;
    }
    
    /**
     * Treat the value as a null-character separated list of tokens.
     * 
     * @return The value tokenized using the null-character ('\u0000')
     */
    public String[] getValueAsArray() {
        return value.split("\u0000");
    }

    @Override
    public String toString() {
        return "ExtraInfo{" + "pv=" + metadata.getName() + ", timestamp=" + timestamp + ", type=" + type + ", value=" + value + '}';
    }
}
