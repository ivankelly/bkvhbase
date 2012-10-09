package com.yahoo.yrl.bcn;

import java.io.IOException;

public interface Adaptor {
    int getNumShards();
    void addEntry(int shard, byte[] data) throws IOException;
}