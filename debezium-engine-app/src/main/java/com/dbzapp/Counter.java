package com.dbzapp;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.json.JSONObject;

/**
 * Helper class to count the number of operations.
 * 
 * @author Vaibhav Kushwaha
 */
public class Counter {
    private long creates;
    private long updates;
    private long deletes;
    private long reads;
    private long tombstones;

    public Counter() {
        this.creates = 0L;
        this.updates = 0L;
        this.deletes = 0L;
        this.reads = 0L;
        this.tombstones = 0L;
    }

    public void updateCount(JSONObject value) {
        if (value != null && !value.toString().isEmpty()) {
            String op = value.getJSONObject("payload").getString("op");
            switch (op) {
                case "r":
                    ++reads;
                    break;
                case "u":
                    ++updates;
                    break;
                case "c":
                    ++creates;
                    break;
                case "d":
                    ++deletes;
                    break;
                default:
                    System.out.println("Record type has an unknown op type: " + op);
            }
        } else {
            ++tombstones;
        }
    }

    public long getCreates() {
        return creates;
    }

    public long getUpdates() {
        return updates;
    }

    public long getDeletes() {
        return deletes;
    }

    public long getReads() {
        return reads;
    }

    public long getTombstones() {
        return tombstones;
    }

    public String toString() {
        return String.format("Creates: %d Updates: %d Deletes: %d Reads: %d Tombstones: %d",
                             creates, updates, deletes, reads, tombstones);
    }

    public void log() {
        System.out.println(toString());
    }
}
