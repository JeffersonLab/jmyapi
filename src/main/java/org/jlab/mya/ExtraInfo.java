package org.jlab.mya;

import java.time.Instant;

/**
 * Provides additional information about an EPICS PV, which unlike standard
 * Metadata, is versioned (may change over time).
 *
 * <p>
 * Extra info types include:
 * </p>
 * <ul>
 * <li>"enum_strings"</li>
 * <li>"notes"</li>
 * </ul>
 *
 * @author slominskir
 */
public final class ExtraInfo {

    private final Metadata metadata;
    private final Instant timestamp;
    private final String type;
    private final String value;

    /**
     * Create a new ExtraInfo.
     *
     * @param metadata The PV metadata
     * @param type The extra info type
     * @param timestamp The timestamp of the info
     * @param value The extra info value
     */
    public ExtraInfo(Metadata metadata, String type, Instant timestamp, String value) {
        this.metadata = metadata;
        this.type = type;
        this.timestamp = timestamp;
        this.value = value;
    }

    /**
     * Return the PV metadata.
     *
     * @return The metadata
     */
    public Metadata getMetadata() {
        return metadata;
    }

    /**
     * Return the extra info type.
     *
     * @return The type
     */
    public String getType() {
        return type;
    }

    /**
     * Return the timestamp.
     *
     * @return The timestamp
     */
    public Instant getTimestamp() {
        return timestamp;
    }

    /**
     * Return the extra info value.
     *
     * @return The value
     */
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
