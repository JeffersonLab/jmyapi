package org.jlab.mya;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class RingBufferTest {
    /**
     * Test of getMean when the buffer has been filled completely.
     */
    @Test
    public void testGetMeanFull() {
        RingBuffer buffer = new RingBuffer(10);
        buffer.write(1);
        buffer.write(2);
        buffer.write(3);
        buffer.write(4);
        buffer.write(5);
        buffer.write(6);
        buffer.write(7);
        buffer.write(8);
        buffer.write(9);
        buffer.write(10);

        double exp = 5.5;
        double result = buffer.getMean();
        assertEquals(exp, result, 1e-10);
    }

    /**
     * Test of getMean when the buffer has been filled halfway.
     */
    @Test
    public void testGetMeanHalfFull() {
        RingBuffer buffer = new RingBuffer(10);
        buffer.write(1);
        buffer.write(2);
        buffer.write(3);
        buffer.write(4);
        buffer.write(5);

        double exp = 3.0;
        double result = buffer.getMean();
        assertEquals(exp, result, 1e-10);
    }

    /**
     * Test of getMean when the buffer has been filled completely and started overwriting data.
     */
    @Test
    public void testGetMeanOverFull() {
        RingBuffer buffer = new RingBuffer(10);
        buffer.write(1);
        buffer.write(2);
        buffer.write(3);
        buffer.write(4);
        buffer.write(5);
        buffer.write(6);
        buffer.write(7);
        buffer.write(8);
        buffer.write(9);
        buffer.write(10);
        buffer.write(11); // Should overwrite 1
        buffer.write(20); // Should overwrite 2

        double exp = 8.3;
        double result = buffer.getMean();
        assertEquals(exp, result, 1e-10);
    }
}
