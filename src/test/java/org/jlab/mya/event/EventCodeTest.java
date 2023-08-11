package org.jlab.mya.event;

import org.junit.*;

import java.util.Set;

public class EventCodeTest {
    @Test
    public void testGetDataEventCodes() {
        // An event should be exclusively either a disconnect or in the getDataEventCodes
        EventCode[] allCodes = EventCode.values();
        Set<EventCode> dataCodes = EventCode.getDataEventCodes();

        boolean isXOR = true;
        for (EventCode code : allCodes) {
            if (code.isDisconnection()) {
                if (dataCodes.contains(code)) {
                    isXOR = false;
                }
            } else {
                if (!dataCodes.contains(code)) {
                    isXOR = false;
                }
            }
        }
        Assert.assertTrue(isXOR);
    }
}
