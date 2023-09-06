package com.dbzapp;

import io.debezium.connector.yugabytedb.*;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

import org.apache.kafka.connect.json.*;
import org.json.JSONObject;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Runner class to demonstrate Debezium Embedded engine in a class.
 * 
 * @author Sumukh Phalgaonkar, Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class EngineRunner {
  private final CmdLineOpts config;
  private final Counter counter;

  public EngineRunner(CmdLineOpts config) {
    this.config = config;
    this.counter = new Counter();
  }

  public void run() throws Exception {
    final Properties props = config.asProperties();
    props.setProperty("name", "engine");
    props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
    props.setProperty("offset.storage.file.filename", "/tmp/offsets.dat");
    props.setProperty("offset.flush.interval.ms", "60000");

    // Create the engine with this configuration ...
    try (DebeziumEngine<ChangeEvent<String, String>> engine = DebeziumEngine.create(Json.class)
            .using(props)
            .notifying((records, committer) -> {
                for(ChangeEvent<String, String> record: records){
                  String recordValue = record.value().isEmpty() ? "{}" : record.value();
                  JSONObject value = new JSONObject(recordValue);
                  counter.updateCount(value);

                  if (!config.shouldDisableRecordOutput()) {
                    if (config.shouldPrintOnlyPayload()) {
                      System.out.println(value);
                    } else {
                      System.out.println(record);
                    }
                  }

                  if (config.shouldPrintCounters()) {
                    counter.log();
                  }

                  committer.markProcessed((record));
                }
                committer.markBatchFinished();
            }).build()
        ) {
      // Run the engine asynchronously ...
      ExecutorService executor = Executors.newSingleThreadExecutor();
      executor.execute(engine);
    } catch (Exception e) {
      System.out.println("Exception from engine: " + e);
      throw e;
    }
  }
}
