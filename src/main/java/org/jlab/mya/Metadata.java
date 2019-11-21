package org.jlab.mya;

/**
 * The metadata associated with an EPICS PV. The metadata is the "key" required to query for events
 * since it reveals what host to query and what underlying ID to query.
 *
 * @author slominskir
 */
public final class Metadata {

    private final int id;
    private final String name;
    private final String host;
    private final DataType type;
    private final int size;

    /**
     * Create a new Metadata.
     *
     * @param id The unique database ID assigned to the PV
     * @param name The PV name
     * @param host The host on which events are stored for this PV
     * @param type The data type
     * @param size The size of an update (scalar = 1, vector &gt; 1)
     */
    public Metadata(int id, String name, String host, DataType type, int size) {
        this.id = id;
        this.name = name;
        this.host = host;
        this.type = type;
        this.size = size;
    }

    /**
     * Return the unique ID.
     *
     * @return The ID
     */
    public int getId() {
        return id;
    }

    /**
     * Return the PV name.
     *
     * @return The name
     */
    public String getName() {
        return name;
    }

    /**
     * Return the host name where the PV's events are located.
     *
     * @return The host name
     */
    public String getHost() {
        return host;
    }

    /**
     * Return the data type of the PV.
     *
     * @return The data type
     */
    public DataType getType() {
        return type;
    }

    /**
     * Return the size of each event (scalar or vector).
     *
     * @return The size
     */
    public int getSize() {
        return size;
    }

    /**
     * Returns a String representation of this Metadata.
     * 
     * @return The String representation
     */
    @Override
    public String toString() {
        return "Metadata{" + "id=" + id + ", name=" + name + ", host=" + host + ", type=" + type
                + ", size=" + size + '}';
    }

    /**
     * Returns a hash code value for the object. This method is supported for the benefit of hash tables such as those provided by HashMap. 
     * 
     * @return A hash code value for this Metadata 
     */
    @Override
    public int hashCode() {
        return id;
    }

    /**
     * Indicates whether some other object is "equal to" this one. The database ID is used for
     * comparison.
     *
     * @param obj The reference object with which to compare
     * @return true if this Metadata is the same as the obj argument; false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Metadata other = (Metadata) obj;
        return this.id == other.id;
    }
}
