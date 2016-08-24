package com.cloudwick.training;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by divya on 8/15/16.
 */
public class Parser extends BaseOperator {
    public transient final DefaultOutputPort<String> output = new DefaultOutputPort<>();
    public transient final DefaultInputPort<String> input = new DefaultInputPort<String>() {
        @Override
        public void process(String s) {
            String[] words = s.split(" ");
            for (String word : words) {
                output.emit(word);
            }
        }
    };
}
