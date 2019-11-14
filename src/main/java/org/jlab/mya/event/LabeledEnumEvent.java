package org.jlab.mya.event;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import org.jlab.mya.EventCode;
import org.jlab.mya.ExtraInfo;
import org.jlab.mya.TimeUtil;

/**
 * Represents a Mya history event for a PV of data type enum, which has been assigned a label.
 * 
 * @author slominskir
 */
public class LabeledEnumEvent extends IntEvent {

    private final String label;

    /**
     * Create a new LabeledEnumEvent.
     * 
     * @param timestamp The event Mya timestamp
     * @param code The event type code
     * @param value The event value
     * @param label The enum label
     */
    public LabeledEnumEvent(long timestamp, EventCode code, int value, String label) {
        super(timestamp, code, value);

        this.label = label;
    }

        /**
     * Create a new LabeledEnumEvent.
     * 
     * @param timestamp The event timestamp
     * @param code The event type code
     * @param value The event value
     * @param label The enum label
     */
    public LabeledEnumEvent(Instant timestamp, EventCode code, int value, String label) {
        super(timestamp, code, value);

        this.label = label;
    }

    /**
     * Return the enum label.
     * 
     * @return The label
     */
    public String getLabel() {
        return label;
    }

    /**
     * Return a String representation of this IntEvent using a timestamp with
     * zero fractional seconds displayed.
     *
     * @return A String representation
     */
    @Override
    public String toString() {
        return toString(0);
    }

    /**
     * Return a String representation of this IntEvent using a timestamp with
     * the specified fractional second precision displayed. Note: using a value
     * of 6 (microseconds) is generally the max precision (A precision up to 9,
     * nanoseconds, is supported, but rounding errors prevent proper
     * conversion).
     *
     * @param f The fractional seconds (-f in myget)
     * @return The String representation
     */
    @Override
    public String toString(int f) {
        String format = TimeUtil.getFractionalSecondsTimestampFormat(f);

        String result = this.getTimestampAsInstant().atZone(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern(format))
                + " ";

        if (code == EventCode.UPDATE) {
            result = result + (label == null ? String.valueOf(value) : label);
        } else {
            result = result + "<" + code.getDescription() + ">";
        }

        return result;
    }

    /**
     * Find the enum label given the int value and the history labels. NOTE: the
     * enumLabelList may be modified as old enum labels that are not used are
     * dropped from the list as an optimization for future calls, which are
     * assumed to be made in asc order.
     *
     * @param iEvent The IntEvent to be labeled
     * @param enumLabelList The list of historical enum labels; which may be
     * modified when the call completes
     * @return A LabeledEnumEvent
     */
    public static LabeledEnumEvent findLabelFromHistory(IntEvent iEvent, List<ExtraInfo> enumLabelList) {

        LabeledEnumEvent lEvent = null;

        if (iEvent != null) {

            int value = iEvent.getValue();
            Instant timestamp = iEvent.getTimestampAsInstant();
            String label = null;

            // We just assume enumLabelList is sorted asc; I hope we're right!
            // We also assume events are sorted asc;  I hope we're right!
            if (enumLabelList != null) {
                int skipped = 0;

                for (int i = 0; i < enumLabelList.size(); i++) {
                    ExtraInfo info = enumLabelList.get(i);
                    boolean infoLessThanOrEqual = info.getTimestamp().isBefore(timestamp) || info.getTimestamp().equals(timestamp);
                    boolean nextInfoGreaterThan = true;
                    if (enumLabelList.size() > i + 1) { // has next
                        ExtraInfo next = enumLabelList.get(i + 1);
                        if (next.getTimestamp().isBefore(timestamp) || next.getTimestamp().equals(timestamp)) {
                            nextInfoGreaterThan = false; // Keep looking
                            skipped++;
                        }
                    }
                    if (infoLessThanOrEqual && nextInfoGreaterThan) {
                        String[] labelArray = info.getValueAsArray();
                        boolean valueInRange = labelArray.length > value;

                        if (valueInRange) {
                            label = labelArray[value];
                        }

                        break;
                    }
                }

                // This optimization just says if we've already passed old historical enum labels disgard them so they aren't considered in future events
                for (int i = 0; i < skipped; i++) {
                    enumLabelList.remove(0);
                }
            }
            
            System.out.println("Found label: " + label);

            lEvent = new LabeledEnumEvent(timestamp, iEvent.getCode(), value, label);
        }

        return lEvent;
    }

    /**
     * Deep Copy Event, but at a new instant in time.
     *
     * @return A new Event
     */
    @Override
    public LabeledEnumEvent copyTo(Instant timeAsInstant) {
        return new LabeledEnumEvent(timeAsInstant, this.getCode(), this.getValue(), this.getLabel());
    }
}
