package org.jlab.mya.jmyapi;

/**
 *
 * @author ryans
 */
public final class PvMetadata {
    private final int id;
    private final String name;
    private final String host;

    public PvMetadata(int id, String name, String host) {
        this.id = id;
        this.name = name;
        this.host = host;
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

    @Override
    public String toString() {
        return "PvMetadata{" + "id=" + id + ", name=" + name + ", host=" + host + '}';
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
        final PvMetadata other = (PvMetadata) obj;
        if (this.id != other.id) {
            return false;
        }
        return true;
    }
}
