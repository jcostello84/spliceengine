package com.splicemachine.stream;

import com.google.common.base.Function;

import java.io.IOException;

/**
 * Represents a Stream of values, each of which can be encountered one element at a time.
 *
 * @author Scott Fines
 * Date: 8/13/14
 */
public interface Stream<T> {

    /**
     * @return the next element in the stream, or {@code null} if no such element exists.
     * @throws IOException if something breaks during the next() call.
     */
    T next() throws StreamException;

    <V> Stream<V> transform(Transformer<T,V> transformer);

    void forEach(Accumulator<T> accumulator) throws StreamException;
}
