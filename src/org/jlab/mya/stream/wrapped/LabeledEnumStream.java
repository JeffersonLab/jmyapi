package org.jlab.mya.stream.wrapped;

import java.io.IOException;
import java.time.Instant;
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
        
        this.enumLabelList = enumLabelList;
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

            if (enumLabelList != null) {
                for (int i = 0; i < enumLabelList.size(); i++) {
                    ExtraInfo info = enumLabelList.get(i);
                    boolean enumStrings = "enum_strings".equals(info.getType());
                    boolean infoLessThanOrEqual = info.getTimestamp().isBefore(timestamp) || info.getTimestamp().equals(timestamp);
                    boolean nextInfoGreaterThan = true;
                    if (enumLabelList.size() > i + 1) { // has next
                        ExtraInfo next = enumLabelList.get(i + 1);
                        if (next.getTimestamp().isBefore(timestamp) || next.getTimestamp().equals(timestamp)) {
                            nextInfoGreaterThan = false; // Keep looking
                        }
                    }
                    if (enumStrings && infoLessThanOrEqual && nextInfoGreaterThan) {
                        String[] labelArray = info.getValueAsArray();
                        boolean valueInRange = labelArray.length > value;

                        if (valueInRange) {
                            label = labelArray[value];
                        }

                        break;
                    }
                }
            }

            lEvent = new LabeledEnumEvent(iEvent.getTimestamp(), iEvent.getCode(), iEvent.getValue(), label);
        }

        return lEvent;
    }
}
