package com.cloudwick.training;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyValPair;
import org.apache.commons.lang.mutable.MutableInt;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by divya on 8/15/16.
 */
public class Counter extends BaseOperator {
    public transient final DefaultOutputPort<KeyValPair<String, Integer>> output = new DefaultOutputPort<KeyValPair<String, Integer>>() {
        @Override
        public Unifier getUnifier() {
            return super.getUnifier();
        }

        public void setup(Context.PortContext context) {
            context.getValue(Context.PortContext.IS_OUTPUT_UNIFIED);
        }
    };
    Map<String, MutableInt> counts = new HashMap<>();
    boolean dataReceived;
    public transient final DefaultInputPort<String> input = new DefaultInputPort<String>() {
        @Override
        public void process(String s) {
            MutableInt count = counts.get(s);
            if (count == null) {
                count = new MutableInt();
                counts.put(s, count);
            }
            count.increment();
            dataReceived = true;
        }
    };
    boolean unifierPresent;

    @Override
    public void endWindow() {
        if (dataReceived) {
            for (Map.Entry<String, MutableInt> entry : counts.entrySet()) {
                output.emit(new KeyValPair<String, Integer>(entry.getKey(), entry.getValue().toInteger()));
            }
            dataReceived = false;
        }
        if (unifierPresent) {
            counts.clear();
        }
    }

    public static class CountUnifier extends Counter implements Unifier<KeyValPair<String, Integer>> {
        @Override
        public void process(KeyValPair<String, Integer> tuple) {
            MutableInt count = counts.get(tuple.getKey());
            if (count == null) {
                count = new MutableInt();
                counts.put(tuple.getKey(), count);
            }
            count.add(tuple.getValue());
            dataReceived = true;

        }

        @Override
        public void endWindow() {
            if (dataReceived) {
                for (Map.Entry<String, MutableInt> entry : counts.entrySet()) {
                    output.emit(new KeyValPair<String, Integer>(entry.getKey(), entry.getValue().toInteger()));
                }
                dataReceived = false;
            }
        }
    }

}
