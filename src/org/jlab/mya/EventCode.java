package org.jlab.mya;

/**
 * Mya event codes.
 * 
 * @author slominskir
 */
public enum EventCode {
    UPDATE("Normal channel data point"),
    NETWORK_DISCONNECTION("Network disconnection"),
    ARCHIVING_OF_CHANNEL_TURNED_OFF("Archiving of channel turned off"),
    ARCHIVER_SHUTDOWN("Archiver shutdown"),
    UNKNOWN_UNAVAILABILTY("Unknown unavailability"),
    ORIGIN_OF_CHANNELS_HISTORY("Origin of channel's history"),
    CHANNELS_PRIOR_DATA_MOVED_OFFLINE("Channel's prior data moved offline"),
    CHANNELS_PRIOR_DATA_DISCARDED("Channel's prior data discarded"),
    UNDEFINED("undefined");
    
    private final String description;
    
    /**
     * Create a new EventCode enum value with the specified description.
     * 
     * @param description The description, which is intended to match the core C++ based MYA tools
     */
    private EventCode(String description) {
        this.description = description;
    }

    /**
     * Return the EventCode description.
     * 
     * @return The description
     */
    public String getDescription() {
        return description;
    }
    
    /**
     * Convert a Mya event code number (as found in the database) to enum value.
     * 
     * @param number The code number
     * @return The EventCode enum value
     */
    public static EventCode fromInt(int number) {
        EventCode code;
        switch(number) {
            case 0:
                code = EventCode.UPDATE;
                break;
            case 1:
                code = EventCode.NETWORK_DISCONNECTION;
                break;
            case 2:
                code = EventCode.ARCHIVING_OF_CHANNEL_TURNED_OFF;
                break;
            case 3:
                code = EventCode.ARCHIVER_SHUTDOWN;
                break;
            case 4:
                code = EventCode.UNKNOWN_UNAVAILABILTY;
                break;
            case 16:
                code = EventCode.ORIGIN_OF_CHANNELS_HISTORY;
                break;
            case 32:
                code = EventCode.CHANNELS_PRIOR_DATA_MOVED_OFFLINE;
                break;
            case 48:
                code = EventCode.CHANNELS_PRIOR_DATA_DISCARDED;
                break;
            default:
                throw new IllegalArgumentException("Unknown code number: " + number);
        }
        
        return code;
    }
}
