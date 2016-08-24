package com.cloudwick.training;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.TopNSort;

import javax.validation.constraints.Min;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by divya on 8/17/16.
 */
public class TopN extends BaseOperator {

    public transient final DefaultOutputPort<List<Map<String, Object>>> outputData = new DefaultOutputPort<>();
    @Min(1)
    int n;
    TopNSort<SortedPair> topNSort;
    public transient final DefaultInputPort<KeyValPair<String, Integer>> input = new DefaultInputPort<KeyValPair<String, Integer>>() {
        @Override
        public void process(KeyValPair<String, Integer> tuple) {
            SortedPair sortedPair = new SortedPair(tuple.getKey(), tuple.getValue());
            topNSort.add(sortedPair);
        }
    };

    @Override
    public void setup(Context.OperatorContext context) {
        topNSort = new TopNSort<SortedPair>(n, n, true);
    }

    @Override
    public void endWindow() {
        if (!topNSort.isEmpty()) {
            List<Map<String, Object>> dataPoints = new ArrayList<>();
            List<SortedPair> topN = topNSort.getTopN(n);
            for (SortedPair pair : topN) {
                Map<String, Object> dataPoint = new HashMap<>();
                dataPoint.put("word", pair.word);
                dataPoint.put("count", pair.value);
                dataPoints.add(dataPoint);
            }
            outputData.emit(dataPoints);
        }
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    private class SortedPair implements Comparable<SortedPair> {
        String word;
        Integer value;

        public SortedPair(String word, Integer value) {
            this.word = word;
            this.value = value;
        }

        @Override
        public int compareTo(SortedPair o) {
            return value.compareTo(o.value);
        }
    }
}
