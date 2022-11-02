package org.jlab.mya.event;

/**
 * Mya event codes. There are two types of events: updates and informational.
 * Some informational event types are also intended to represent disconnections,
 * or gaps in event history.
 *
 * @author slominskir
 */
public enum EventCode {
    /**
     * Update
     */
    UPDATE(0, "Normal channel data point", false),
    /**
     * Network Disconnection
     */
    NETWORK_DISCONNECTION(1, "Network disconnection", true),
    /**
     * Archiving Disabled
     */
    ARCHIVING_OF_CHANNEL_TURNED_OFF(2, "Archiving of channel turned off", true),
    /**
     * Archiver Shutdown
     */
    ARCHIVER_SHUTDOWN(3, "Archiver shutdown", true),
    /**
     * Unknown
     */
    UNKNOWN_UNAVAILABILTY(4, "Unknown unavailability", true),
    /**
     * NaN or Infinity
     */
    NAN_OR_INFINITY(5, "NaN/infinity encountered", false),
    /**
     * Origin
     */
    ORIGIN_OF_CHANNELS_HISTORY(16, "Origin of channel's history", false),
    /**
     * Offline
     */
    CHANNELS_PRIOR_DATA_MOVED_OFFLINE(32, "Channel's prior data moved offline", false),
    /**
     * Discarded
     */
    CHANNELS_PRIOR_DATA_DISCARDED(48, "Channel's prior data discarded", false),
    /**
     * Undefined
     */
    UNDEFINED(128, "undefined", false);

    private final int codeNumber;
    private final String description;
    private final boolean disconnection;

    /**
     * Create a new EventCode enum value with the specified description.
     *
     * @param codeNumber The event code number
     * @param description The description, which is intended to match the core
     * C++ based MYA tools
     * @param disconnection Whether the event represents a disconnection
     */
    private EventCode(int codeNumber, String description, boolean disconnection) {
        this.codeNumber = codeNumber;
        this.description = description;
        this.disconnection = disconnection;
    }

    /**
     * Return the code number of the EventCode.
     *
     * @return The code number
     */
    public int getCodeNumber() {
        return codeNumber;
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
     * Return whether the EventCode represents a "disconnection" event.
     *
     * @return true if a disconnection event, false otherwise
     */
    public boolean isDisconnection() {
        return disconnection;
    }

    /**
     * Convert a Mya event code number (as found in the database) to enum value.
     *
     * @param number The code number
     * @return The EventCode enum value
     */
    public static EventCode fromInt(int number) {
        EventCode code;
        switch (number) {
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
