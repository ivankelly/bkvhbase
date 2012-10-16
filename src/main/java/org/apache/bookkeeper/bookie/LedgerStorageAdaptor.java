package org.apache.bookkeeper.bookie;

import com.yahoo.yrl.bcn.Adaptor;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.ActiveLedgerManager;

import java.nio.ByteBuffer;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LedgerStorageAdaptor implements Adaptor {
    static Logger LOG = LoggerFactory.getLogger(LedgerStorageAdaptor.class);

    final byte[] COMMON_KEY = "COMMON_KEY".getBytes();
    final LedgerStorage ls;
    final SyncThread syncThread;
    final AtomicLong[] ledgerIndices;


    public LedgerStorageAdaptor(File base, int numRegions) throws Exception {
        ServerConfiguration conf = new ServerConfiguration();
        conf.setLedgerDirNames(new String[] { base.toString() });
        File curDir = Bookie.getCurrentDirectory(base);
        if (!curDir.exists()) {
            curDir.mkdirs();
        }
        
        ls = new InterleavedLedgerStorage(conf, new DummyActiveLedgerManager());

        ledgerIndices = new AtomicLong[numRegions];
        for (int i = 0; i < numRegions; i++) {
            ledgerIndices[i] = new AtomicLong(0);
        }

        for (int i = 0; i < numRegions; i++) {
            ls.setMasterKey(i, COMMON_KEY);
        }
        syncThread = new SyncThread(conf);
        syncThread.setDaemon(true);
    }

    public int getNumShards() {
        return ledgerIndices.length;
    }

    public void addEntry(int shard, byte[] data) throws IOException {
        ByteBuffer b = ByteBuffer.allocate(16 + data.length);
        b.putLong((long)shard);
        b.putLong((long)ledgerIndices[shard].incrementAndGet());
        b.put(data);
        b.flip();

        ls.addEntry(b);
    }

    public void readEntries(int shard, Reader r) throws IOException {
        long eid = 0;
        while (true) {
            try {
                ByteBuffer b = ls.getEntry(shard, ++eid);
                b.getLong();
                b.getLong();
                byte[] bytes = new byte[b.remaining()];
                b.get(bytes);
                r.entryRead(bytes);
            } catch (Bookie.NoEntryException nne) {
                r.done();
                // reached end
                return;
            }
        }
    }

    @Override
    public void start() throws IOException {
        syncThread.start();
    }

    @Override
    public void shutdown() throws IOException {
        ls.flush();
        try {
            syncThread.shutdown();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException(ie);
        }
    }

    class SyncThread extends Thread {
        volatile boolean running = true;
        // flag to ensure sync thread will not be interrupted during flush
        final AtomicBoolean flushing = new AtomicBoolean(false);
        // make flush interval as a parameter
        final int flushInterval;

        public SyncThread(ServerConfiguration conf) {
            super("SyncThread");
            flushInterval = conf.getFlushInterval();
            LOG.debug("Flush Interval : {}", flushInterval);
        }

        @Override
        public void run() {
            while(running) {
                synchronized(this) {
                    try {
                        wait(flushInterval);
                        if (!ls.isFlushRequired()) {
                            continue;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        continue;
                    }
                }

                // try to mark flushing flag to make sure it would not be interrupted
                // by shutdown during flushing. otherwise it will receive
                // ClosedByInterruptException which may cause index file & entry logger
                // closed and corrupted.
                if (!flushing.compareAndSet(false, true)) {
                    // set flushing flag failed, means flushing is true now
                    // indicates another thread wants to interrupt sync thread to exit
                    break;
                }

                try {
                    ls.flush();
                } catch (IOException e) {
                    LOG.error("Exception flushing Ledger", e);
                }

                flushing.set(false);
            }
        }

        // shutdown sync thread
        void shutdown() throws InterruptedException {
            running = false;
            if (flushing.compareAndSet(false, true)) {
                // if setting flushing flag succeed, means syncThread is not flushing now
                // it is safe to interrupt itself now 
                this.interrupt();
            }
            this.join();
        }
    }

    class DummyActiveLedgerManager implements ActiveLedgerManager {
        public void addActiveLedger(long ledgerId, boolean active) {}
        public void removeActiveLedger(long ledgerId) {}
        public boolean containsActiveLedger(long ledgerId) { return true; }
        public void garbageCollectLedgers(GarbageCollector gc) {}
        public void close() throws IOException {}
    }
}