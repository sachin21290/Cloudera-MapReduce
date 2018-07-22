package com.mapreduce.utility.reporting.keyfn;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.Pair;
import com.mapreduce.trace.Trace;
import com.mapreduce.utility.reporting.model.Constants;

public class LogKeyFn extends DoFn<Trace, Pair<String, Trace>> {

    private static final long serialVersionUID = -3174116713617363522L;

    @Override
    public void process(Trace trace, Emitter<Pair<String, Trace>> emitter) {
        String traceType = trace.getTraceType().name();
        if (traceType.equalsIgnoreCase(Constants.LOCALITY_RULE_SOURCE_PLACE)) {
            String objectId = new StringBuilder(trace.getObjectId()).toString();
            emitter.emit(Pair.of(objectId, trace));
        }
    }

}
