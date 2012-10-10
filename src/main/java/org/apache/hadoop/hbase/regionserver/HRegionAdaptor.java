package org.apache.hadoop.hbase.regionserver;

import java.util.concurrent.atomic.AtomicInteger;

import com.yahoo.yrl.bcn.Adaptor;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.regionserver.metrics.RegionServerMetrics;

import java.lang.Thread.UncaughtExceptionHandler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.lang.reflect.Method;
import java.lang.reflect.Field;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HRegionAdaptor implements Adaptor {
    static Logger LOG = LoggerFactory.getLogger(HRegionAdaptor.class);

    final HRegionServer server;
    final byte[][] regionNames;
    final AtomicInteger[] regionIndices;
    final Configuration conf;
    final static String FAMILY = "DATAFAM";
    final static String QUALIFIER = "DATA";

    public HRegionAdaptor(File baseDirectory, int numRegions) throws Exception {
        conf = HBaseConfiguration.create();
        server = new HRegionServer(conf);
        initializeThreads();

        regionNames = new byte[numRegions][];
        regionIndices = new AtomicInteger[numRegions];

        Configuration conf = new Configuration();
        for (int i = 0; i < numRegions; i++) {
            regionIndices[i] = new AtomicInteger(0);
            HRegion r = initHRegion(conf, new File(baseDirectory, "region" + i),
                                    Bytes.toBytes("table"+i),
                                    server,
                                    Bytes.toBytes(FAMILY));
            regionNames[i] = r.getRegionName();
            server.addToOnlineRegions(r);
        }
    }

    void initializeThreads() throws Exception {
        Method m = HRegionServer.class.getDeclaredMethod("initializeThreads", null);
        m.setAccessible(true);
        m.invoke(server, null);

        Field metrics = HRegionServer.class.getDeclaredField("metrics");
        metrics.setAccessible(true);
        metrics.set(server, new RegionServerMetrics());

        String n = Thread.currentThread().getName();
        UncaughtExceptionHandler handler = new UncaughtExceptionHandler() {
                public void uncaughtException(Thread t, Throwable e) {
                    LOG.error("Uncaught exception in service thread " + t.getName(), e);
                }
            };
        Threads.setDaemonThreadRunning(server.cacheFlusher.getThread(), n + ".cacheFlusher",
                                       handler);
        Threads.setDaemonThreadRunning(server.compactionChecker.getThread(), n +
                                       ".compactionChecker", handler);
    }

    @Override
    public int getNumShards() {
        return regionNames.length;
    }

    @Override
    public void addEntry(int regionIndex, byte[] data) throws IOException {
        if (regionIndex >= regionNames.length || regionIndex < 0) {
            throw new IOException("Bad region index");
        }
        Put put = new Put(Bytes.toBytes("entry" + regionIndices[regionIndex].incrementAndGet()));
        put.setWriteToWAL(false);
        put.add(Bytes.toBytes(FAMILY), Bytes.toBytes(QUALIFIER), data);

        byte[] regionName = regionNames[regionIndex];
        server.put(regionName, put);
    }

    private static HRegion initHRegion(Configuration conf, File directory, byte[] tableName,
                                       RegionServerServices services,
                                       byte[]... families) throws IOException {
        HTableDescriptor htd = new HTableDescriptor(tableName);
        for(byte [] family : families) {
            htd.addFamily(new HColumnDescriptor(family));
        }

        HRegionInfo info = new HRegionInfo(htd.getName(), null/*startKey*/, null/*stopKey*/, false);
        Path path = new Path(directory.toURI());
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(path)) {
            if (!fs.delete(path, true)) {
                throw new IOException("Failed delete of " + path);
            }
        }

        HRegion r = new HRegion(path, null, fs, conf, info, htd, services);
        r.initialize();
        return r;
    }
}