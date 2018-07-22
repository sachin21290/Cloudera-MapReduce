package com.mapreduce.utility.reporting.props;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class PopulateParameters {

    private PopulateParameters() {

    }

    @Option(required = true, name = "-rundescList",
        usage = "HDFS Path to rundescriptor lists providing the list of rundescriptors that needs to be added to the schema")
    private String rundescList;

    public static PopulateParameters parseArguments(String[] args) throws CmdLineException {
        PopulateParameters params = new PopulateParameters();
        final CmdLineParser parser = new CmdLineParser(params);
        try {
            parser.parseArgument(args);
            return params;
        } catch (final CmdLineException e) {
            parser.printUsage(System.err);
            throw e;
        }
    }

    public String getRundescriptorList() {
        return rundescList;
    }

    public void setRundescList(String rundescList) {
        this.rundescList = rundescList;
    }

}
