package org.apache.hadoop.hbase.regionserver;

import java.util.concurrent.atomic.AtomicInteger;

import com.yahoo.yrl.bcn.Adaptor;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Result;
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
    final HRegion[] regions;
    final Configuration conf;
    final static String FAMILY = "DATAFAM";
    final static String QUALIFIER = "DATA";

    public HRegionAdaptor(File baseDirectory, int numRegions, boolean clearData) throws Exception {
        conf = HBaseConfiguration.create();
        conf.setInt("hbase.regionserver.regionSplitLimit", numRegions);
        server = new HRegionServer(conf);

        regionNames = new byte[numRegions][];
        regions = new HRegion[numRegions];
        regionIndices = new AtomicInteger[numRegions];

        Configuration conf = new Configuration();
        for (int i = 0; i < numRegions; i++) {
            regionIndices[i] = new AtomicInteger(0);
            regions[i] = initHRegion(conf, new File(baseDirectory, "region" + i),
                                    Bytes.toBytes("table"+i),
                                    server, clearData,
                                    Bytes.toBytes(FAMILY));
            regionNames[i] = regions[i].getRegionName();
            server.addToOnlineRegions(regions[i]);
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

    @Override
    public void readEntries(int regionIndex, Reader r) throws IOException {
        byte[] regionName = regionNames[regionIndex];

        Scan s = new Scan();
        s.addColumn(Bytes.toBytes(FAMILY), Bytes.toBytes(QUALIFIER));
        long scanId = server.openScanner(regionName, s);
        Result res = server.next(scanId);
        while (res != null) {
            r.entryRead(res.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes(QUALIFIER)));
            res = server.next(scanId);
        }
        r.done();
    }
    
    @Override
    public void start() throws IOException {
        try {
            initializeThreads();
        } catch (Exception e) {
            throw new IOException("Couldnt initialize threads", e);
        }
    }

    @Override
    public void logImplStats(Logger logger) {}

    @Override
    public void shutdown() throws IOException {
        for (HRegion r : regions) {
            r.close(false);
        }
    }

    private static HRegion initHRegion(Configuration conf, File directory, byte[] tableName,
                                       RegionServerServices services, boolean clearData,
                                       byte[]... families) throws IOException {
        HTableDescriptor htd = new HTableDescriptor(tableName);
        for(byte [] family : families) {
            htd.addFamily(new HColumnDescriptor(family));
        }

        HRegionInfo info = new HRegionInfo(htd.getName(), null/*startKey*/, null/*stopKey*/,
                false, (long)new String(tableName).hashCode());
        LOG.info("HREgion info {} {}", info.getEncodedName(), htd.getName());
        Path path = new Path(directory.toURI());
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(path) && clearData) {
            if (!fs.delete(path, true)) {
                throw new IOException("Failed delete of " + path);
            }
        }

        HRegion r = new HRegion(path, null, fs, conf, info, htd, services);
        r.initialize();

        return r;
    }
}