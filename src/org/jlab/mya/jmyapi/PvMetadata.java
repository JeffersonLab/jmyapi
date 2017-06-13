package org.jlab.mya.jmyapi;

/**
 *
 * @author ryans
 */
public final class PvMetadata {
    private final int id;
    private final String name;
    private final String host;
    private final PvDataType type;
    private final int size;

    public PvMetadata(int id, String name, String host, PvDataType type, int size) {
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

    public PvDataType getType() {
        return type;
    }

    public int getSize() {
        return size;
    }

    @Override
    public String toString() {
        return "PvMetadata{" + "id=" + id + ", name=" + name + ", host=" + host + ", type=" + type +
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
        final PvMetadata other = (PvMetadata) obj;
        if (this.id != other.id) {
            return false;
        }
        return true;
    }
    
    public Class getTypeClass() {
        Class clazz;
        
        switch(type) {
            case DBR_STRING:
                clazz = String.class;
                break;
            case DBR_SHORT:
                clazz = Short.class;
                break;
            case DBR_FLOAT:
                clazz = Float.class;
                break;
            case DBR_ENUM:
                clazz = Integer.class;
                break;
            case DBR_CHAR:
                clazz = Integer.class;
                break;
            case DBR_LONG:
                clazz = Long.class;
                break;
            case DBR_DOUBLE: // Use float for double per MYA precedent
                clazz = Float.class;
                break;
            default: 
                clazz = null;
        }
        
        return clazz;
    }
}
