/**
 * Put your copyright and license info here.
 */
package com.cloudwick.training;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is a simple operator that emits random number.
 */
public class RandomNumberGenerator extends BaseOperator implements InputOperator {
    public final transient DefaultOutputPort<Double> out = new DefaultOutputPort<Double>();
    private int numTuples = 100;
    private transient int count = 0;

    @Override
    public void beginWindow(long windowId) {
        count = 0;
    }

    @Override
    public void emitTuples() {
        if (count++ < numTuples) {
            out.emit(Math.random());
        }
    }

    public int getNumTuples() {
        return numTuples;
    }

    /**
     * Sets the number of tuples to be emitted every window.
     *
     * @param numTuples number of tuples
     */
    public void setNumTuples(int numTuples) {
        this.numTuples = numTuples;
    }
}
