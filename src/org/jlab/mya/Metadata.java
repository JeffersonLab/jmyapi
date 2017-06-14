package org.jlab.mya;

/**
 *
 * @author ryans
 */
public final class Metadata {
    private final int id;
    private final String name;
    private final String host;
    private final DataType type;
    private final int size;

    public Metadata(int id, String name, String host, DataType type, int size) {
        this.id = id;
        this.name = name;
        this.host = host;
        this.type = type;
        this.size = size;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getHost() {
        return host;
    }

    public DataType getType() {
        return type;
    }

    public int getSize() {
        return size;
    }

    @Override
    public String toString() {
        return "Metadata{" + "id=" + id + ", name=" + name + ", host=" + host + ", type=" + type +
                ", size=" + size + '}';
    }

    @Override
    public int hashCode() {
        return id;
    }

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
        if (this.id != other.id) {
            return false;
        }
        return true;
    }    
}
