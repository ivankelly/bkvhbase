package com.yahoo.yrl.bcn;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.ParseException;

import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.regionserver.HRegionAdaptor;

public class BKvHBase {
    static Logger LOG = LoggerFactory.getLogger(BKvHBase.class);

    
    private void runBench(int rate, int seconds, HRegionAdaptor adaptor) throws Exception {
        
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("BKvHBase <options>", options);
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("hregion", false, "Benchmark hbase region storage");
        options.addOption("time", true, "Time to run for, in seconds, default 60");
        options.addOption("rate", true, "Rate at which to write requests, default 1000");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("help")) {
            printHelp(options);
            System.exit(-1);
        }

        Adaptor adaptor = null;
        if (cmd.hasOption("hregion")) {
            adaptor = new HRegionAdaptor(new File("/tmp/testhbase"), 1);
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

        b.run();
    }
}