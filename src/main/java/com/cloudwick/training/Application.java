/**
 * Put your copyright and license info here.
 */
package com.cloudwick.training;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.snapshot.AppDataSnapshotServerMap;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;

@ApplicationAnnotation(name = "DivyaApplication")
public class Application implements StreamingApplication {

    @Override
    public void populateDAG(DAG dag, Configuration conf) {
        // Sample DAG with 2 operators
        // Replace this code with the DAG you want to build

        //AbstractFileInputOperator.FileLineInputOperator fileInput = dag.addOperator("input", AbstractFileInputOperator.FileLineInputOperator.class);
        KafkaSinglePortStringInputOperator KafkaInput = dag.addOperator("input", KafkaSinglePortStringInputOperator.class);
        Parser parser = dag.addOperator("parser", Parser.class);
        Counter counter = dag.addOperator("counter", Counter.class);
        //ConsoleOutputOperator console = dag.addOperator("console", ConsoleOutputOperator.class);
        JdbcOutput output = dag.addOperator("output", JdbcOutput.class);
        TopN topN = dag.addOperator("topN", TopN.class);

        String gatewayAddress = dag.getValue(Context.DAGContext.GATEWAY_CONNECT_ADDRESS);
        URI gatewayURI = URI.create("ws://" + gatewayAddress + "/pubsub");

        AppDataSnapshotServerMap dataServer = dag.addOperator("server", AppDataSnapshotServerMap.class);
        dataServer.setSnapshotSchemaJSON(SchemaUtils.jarResourceFileToString("schema.json"));

        PubSubWebSocketAppDataQuery queryProvider = new PubSubWebSocketAppDataQuery();
        queryProvider.setUri(gatewayURI);
        dataServer.setEmbeddableQueryInfoProvider(queryProvider);

        PubSubWebSocketAppDataResult dataResult = dag.addOperator("result", PubSubWebSocketAppDataResult.class);
        dataResult.setUri(gatewayURI);

        dag.addStream("lines", KafkaInput.outputPort, parser.input);
        dag.addStream("words", parser.output, counter.input);
        dag.addStream("counter", counter.output, output.input, topN.input);
        dag.addStream("topN", topN.outputData, dataServer.input);
        dag.addStream("results", dataServer.queryResult, dataResult.input);

    }
}
