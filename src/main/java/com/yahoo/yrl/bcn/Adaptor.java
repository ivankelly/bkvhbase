package com.yahoo.yrl.bcn;

import java.io.IOException;

public interface Adaptor {
    int getNumShards();
    void addEntry(int shard, byte[] data) throws IOException;
    void readEntries(int shard, Reader r) throws IOException;

    void start() throws IOException;
    void shutdown() throws IOException;

    public interface Reader {
        void entryRead(byte[] data);
        void done();
    }
}