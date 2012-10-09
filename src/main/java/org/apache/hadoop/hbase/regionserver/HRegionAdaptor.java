package org.apache.hadoop.hbase.regionserver;

import java.util.concurrent.atomic.AtomicInteger;

import com.yahoo.yrl.bcn.Adaptor;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

public class HRegionAdaptor implements Adaptor {
    final HRegion[] regions;
    final AtomicInteger[] regionIndices;
    final static String FAMILY = "DATAFAM";
    final static String QUALIFIER = "DATA";

    public HRegionAdaptor(File baseDirectory, int numRegions) throws IOException {
        regions = new HRegion[numRegions];
        regionIndices = new AtomicInteger[numRegions];

        Configuration conf = new Configuration();
        for (int i = 0; i < numRegions; i++) {
            regionIndices[i] = new AtomicInteger(0);
            regions[i] = initHRegion(conf, new File(baseDirectory, "region" + i),
                    Bytes.toBytes("table"+i), Bytes.toBytes(FAMILY));
        }
    }

    @Override
    public int getNumShards() {
        return regions.length;
    }

    @Override
    public void addEntry(int regionIndex, byte[] data) throws IOException {
        if (regionIndex >= regions.length || regionIndex < 0) {
            throw new IOException("Bad region index");
        }
        Put put = new Put(Bytes.toBytes("entry" + regionIndices[regionIndex].incrementAndGet()));
        put.setWriteToWAL(false);
        put.add(Bytes.toBytes(FAMILY), Bytes.toBytes(QUALIFIER), data);

        regions[regionIndex].put(put);
    }

    private static HRegion initHRegion(Configuration conf, File directory, byte[] tableName,
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
        return HRegion.createHRegion(info, path, conf, htd);
    }
    
    public SortedMap<Long, HRegion> getCopyOfOnlineRegionsSortedBySize() {
        // we'll sort the regions in reverse
        SortedMap<Long, HRegion> sortedRegions = new TreeMap<Long, HRegion>(
                new Comparator<Long>() {
                    public int compare(Long a, Long b) {
                        return -1 * a.compareTo(b);
                    }
                });
        // Copy over all regions. Regions are sorted by size with biggest first.
        for (HRegion region : regions) {
            sortedRegions.put(Long.valueOf(region.memstoreSize.get()), region);
        }
        return sortedRegions;
    }

    void isStopped() {
    }
}