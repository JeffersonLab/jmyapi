package org.jlab.mya.stream.wrapped;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.jlab.mya.ExtraInfo;
import org.jlab.mya.event.IntEvent;
import org.jlab.mya.event.LabeledEnumEvent;
import org.jlab.mya.stream.IntEventStream;

/**
 *
 * @author ryans
 */
public class LabeledEnumStream extends WrappedEventStreamAdaptor<LabeledEnumEvent, IntEvent> {

    private final List<ExtraInfo> enumLabelList;

    /**
     * Create a new EnumLabelStream by wrapping an IntEventStream.
     *
     * @param stream The IntEventStream to wrap
     * @param enumLabelList The history of enum labels
     */
    public LabeledEnumStream(IntEventStream stream, List<ExtraInfo> enumLabelList) {
        super(stream);

        this.enumLabelList = new ArrayList<>(enumLabelList); // We make a copy because later we may modify the list

        // Verify that extra info is only enum_strings and nothing else
        for (int i = 0; i < enumLabelList.size(); i++) {
            ExtraInfo info = enumLabelList.get(i);
            boolean enumStrings = "enum_strings".equals(info.getType());
            if (!enumStrings) {
                throw new IllegalArgumentException("ExtraInfo must only contain enum_strings");
            }
        }
    }

    /**
     * Read the next event from the stream. Generally you'll want to iterate
     * over the stream using a while loop.
     *
     * @return The next event or null if End-Of-Stream reached
     * @throws IOException If unable to read the next event
     */
    @Override
    public LabeledEnumEvent read() throws IOException {
        IntEvent iEvent = wrapped.read();
        LabeledEnumEvent lEvent = null;

        if (iEvent != null) {

            int value = iEvent.getValue();
            Instant timestamp = iEvent.getTimestamp();
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

            lEvent = new LabeledEnumEvent(timestamp, iEvent.getCode(), value, label);
        }

        return lEvent;
    }
}
