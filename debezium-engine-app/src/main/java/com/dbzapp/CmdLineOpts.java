package com.dbzapp;

import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;

/**
 * Helper class to parse the command line options.
 * 
 * @author Sumukh Phalgaonkar, Vaibhav Kushwaha (vkushwaha@yugabyte.com)
 */
public class CmdLineOpts {
  private final String connectorClass = "io.debezium.connector.yugabytedb.YugabyteDBConnector";
  private String masterAddresses;
  private String hostname;
  private String databasePort = "5433";
  private String streamId;
  private String tableIncludeList;
  private String databaseName = "yugabyte";
  private String databasePassword = "Yugabyte@123";
  private String databaseUser = "yugabyte";
  private String snapshotMode = "never";
  private boolean printOnlyPayload = false;
  private boolean disableRecordOutput = false;
  private boolean printCounters = false;

  public static CmdLineOpts createFromArgs(String[] args) {
    Options options = new Options();

    options.addOption("master_addresses", true, "Addresses of the master process");
    options.addOption("stream_id", true, "DB stream ID");
    options.addOption("table_include_list", true, "The table list to poll for in the form"
                      + " <schemaName>.<tableName>");
    options.addOption("snapshot", false, "Whether to take snapshot");
    options.addOption("print_payload", false, "Print the payload of the records");
    options.addOption("disable_record_output", false, "Do not print any record");
    options.addOption("print_counters", false, "Print the count of each type of record");

    CommandLineParser parser = new DefaultParser();
    CommandLine commandLine = null;
    try {
      commandLine = parser.parse(options, args);
    } catch (Exception e) {
      System.out.println("Exception while parsing arguments: " + e);
      System.exit(-1);
    }

    CmdLineOpts configuration = new CmdLineOpts();
    configuration.initialize(commandLine);
    return configuration;
  }

  private void initialize(CommandLine commandLine) {
    if (commandLine.hasOption("master_addresses")) {
      masterAddresses = commandLine.getOptionValue("master_addresses");
      String[] nodes = masterAddresses.split(",");
      hostname = nodes[0].split(":")[0];
    }

    if (commandLine.hasOption("stream_id")) {
      streamId = commandLine.getOptionValue("stream_id");
    }

    if (commandLine.hasOption("table_include_list")) {
      tableIncludeList = commandLine.getOptionValue("table_include_list");
    }

    if (commandLine.hasOption("snapshot")) {
      snapshotMode = "initial";
    }

    if (commandLine.hasOption("print_payload")) {
      printOnlyPayload = true;
    }

    if (commandLine.hasOption("disable_record_output")) {
      this.disableRecordOutput = true;
    }
    
    if (commandLine.hasOption("print_counters")) {
      this.printCounters = true;
    }
  }

  public boolean shouldPrintOnlyPayload() {
    return printOnlyPayload;
  }

  public boolean shouldDisableRecordOutput() {
    return disableRecordOutput;
  }

  public boolean shouldPrintCounters() {
    return printCounters;
  }

  public Properties asProperties() {
    Properties props = new Properties();
    props.setProperty("connector.class", connectorClass);

    props.setProperty("database.streamid", streamId);
    props.setProperty("database.master.addresses", masterAddresses);
    props.setProperty("table.include.list", tableIncludeList);
    props.setProperty("database.hostname", hostname);
    props.setProperty("database.port", databasePort);
    props.setProperty("database.user", databaseUser);
    props.setProperty("database.password", databasePassword);
    props.setProperty("database.dbname", databaseName);
    props.setProperty("database.server.name", "dbserver1");
    props.setProperty("snapshot.mode", snapshotMode);

    return props;
  }

}
