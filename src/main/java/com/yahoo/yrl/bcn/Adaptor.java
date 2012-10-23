package com.yahoo.yrl.bcn;

import java.io.IOException;

import org.slf4j.Logger;

public interface Adaptor {
    int getNumShards();
    void addEntry(int shard, byte[] data) throws IOException;
    void readEntries(int shard, Reader r) throws IOException;

    void start() throws IOException;
    void shutdown() throws IOException;

    void logImplStats(Logger logger);

    public interface Reader {
        void entryRead(byte[] data);
        void done();
    }
}