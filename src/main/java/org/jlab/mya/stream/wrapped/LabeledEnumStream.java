package org.jlab.mya.stream.wrapped;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jlab.mya.EventStream;
import org.jlab.mya.ExtraInfo;
import org.jlab.mya.event.IntEvent;
import org.jlab.mya.event.LabeledEnumEvent;

/**
 * Wraps an IntEventStream and provides LabelEnumEvents by assigning enumeration
 * labels to integer values.
 *
 * @author slominskir
 */
public class LabeledEnumStream extends WrappedEventStreamAdaptor<LabeledEnumEvent, IntEvent> {

    private final List<ExtraInfo> enumLabelList;

    /**
     * Create a new EnumLabelStream by wrapping an IntEventStream.
     *
     * @param stream The IntEventStream to wrap
     * @param enumLabelList The history of enum labels
     */
    public LabeledEnumStream(EventStream<IntEvent> stream, List<ExtraInfo> enumLabelList) {
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
        return LabeledEnumEvent.findLabelFromHistory(iEvent, enumLabelList);
    }
}
