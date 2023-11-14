package org.jlab.mya;

/**
 * Simple RingBuffer class that tracks doubles and allows only for adding new data and computing simple statistics.
 * Helpful for fast windowed statics while processing an EventStream.
 *
 * @author adamc
 */
public class RingBuffer {
    private final long[] values;
    private int write;
    private int count;

    /**
     * Construct an instance with an internal array of a given size.
     * @param size The size of the array used in the buffer.
     */
    public RingBuffer(int size) {
        write = 0;
        values = new long[size];
        count = 0;
    }

    /**
     * Add a new value to the buffer.  This may overwrite an old value.
     * @param val The value to write to the buffer.
     */
    public void write(long val) {
        values[write++] = val;
        if (write == values.length) {
            write = 0;
        }
        if (count < values.length) {
            count++;
        }
    }

    /**
     * This calculates the mean of the values in the buffer.
     * @return The average value of the data contained in the buffer.
     */
    public double getMean() {
        double sum = 0;
        // Iterate up to count as handles the case where we've written fewer than 'size' values to the buffer.
        for (int i = 0; i < count; i++) {
            sum = sum + values[i];
        }
        return sum / count;
    }
}