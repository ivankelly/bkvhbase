package com.yahoo.yrl.bcn;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.regionserver.HRegionAdaptor;
import org.apache.bookkeeper.bookie.LedgerStorageAdaptor;
import org.apache.bookkeeper.bookie.SlabStorageAdaptor;

public class BKvHBase {
    static Logger LOG = LoggerFactory.getLogger(BKvHBase.class);


    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("BKvHBase <options>", options);
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("hregion", false, "Benchmark hbase region storage");

        options.addOption("ledgerStorage", false, "Benchmark BookKeeper LedgerStorage");
        options.addOption("slabStorage", false, "Benchmark BookKeeper SlabStorage");
        
        options.addOption("time", true, "Time to run for, in seconds, default 60");
        options.addOption("rate", true, "Rate at which to write requests, default 1000");
        options.addOption("size", true, "Size of packets to use");
        options.addOption("shards", true, "Number of shards to write to, default 1");
        options.addOption("directory", true, "Directory to write to");
        options.addOption("read", true, "Run reads workload, reading n shards");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("help")) {
            printHelp(options);
            System.exit(-1);
        }

        int shards = 1;
        
        if (!cmd.hasOption("directory")) {
            LOG.error("Must specify a directory");
            printHelp(options);
            System.exit(-1);
        }
        File dir = new File(cmd.getOptionValue("directory"));
        if (!dir.exists()) {
            dir.mkdirs();
        }

        File metaFile = new File(dir, "META");
        if (cmd.hasOption("read")) {
            Properties p = new Properties();
            FileInputStream i = new FileInputStream(metaFile);
            p.load(i);
            i.close();

            shards = Integer.valueOf(p.getProperty("shards"));
        } else {
            if (metaFile.exists()) {
                LOG.error("Meta file {} already exists, clean the directory before writing", metaFile);
                System.exit(-1);
            }
            if (cmd.hasOption("shards")) {
                shards = Integer.valueOf(cmd.getOptionValue("shards"));
            }            
            Properties p = new Properties();
            p.setProperty("shards", String.valueOf(shards));
            FileOutputStream o = new FileOutputStream(metaFile);
            p.store(o, "benchmarking meta file");
            o.close();
        }

        Adaptor adaptor = null;
        if (cmd.hasOption("hregion")) {
            adaptor = new HRegionAdaptor(dir, shards,
                                         !cmd.hasOption("read"));
        } else if (cmd.hasOption("ledgerStorage")) {
            adaptor = new LedgerStorageAdaptor(dir, shards);
        } else if (cmd.hasOption("slabStorage")) {
            adaptor = new SlabStorageAdaptor(dir, shards);
        } else {
            printHelp(options);
            System.exit(-1);
        }
            
        Bencher b = new Bencher(adaptor);
        if (cmd.hasOption("time")) {
            b.setTime(Integer.valueOf(cmd.getOptionValue("time")));
        }
        if (cmd.hasOption("rate")) {
            b.setRate(Integer.valueOf(cmd.getOptionValue("rate")));
        }
        if (cmd.hasOption("size")) {
            b.setData(new byte[Integer.valueOf(cmd.getOptionValue("size"))]);
        }

        adaptor.start();
        if (cmd.hasOption("read")) {
            b.runReads(Integer.valueOf(cmd.getOptionValue("read")));
        } else {
            b.runWrites();
        }
        adaptor.shutdown();
    }
}