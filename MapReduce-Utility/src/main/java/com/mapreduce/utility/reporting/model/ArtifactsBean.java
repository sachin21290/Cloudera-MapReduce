package com.mapreduce.utility.reporting.model;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Maps;
import com.mapreduce.repository.RunXmlFile;
import com.mapreduce.HdfsTools;

public class ArtifactsBean {

    private String ptTimestamp;
    private String ptVersion;
    private String tag;
    private List<String> localities;
    private Map<String, String> ptCountries;
    private Map<String, String> ptStates;

    public ArtifactsBean(RunXmlFile runXmlFile, Configuration configuration) throws IOException {
        String buildRootPath;
        if (runXmlFile.isReleasable()) {
            buildRootPath = runXmlFile.getArtifactReleasePath();
            this.ptTimestamp = String.valueOf(System.currentTimeMillis());
        } else {
            Path outputPath = new Path(runXmlFile.getRunXmlFilePath()).getParent().getParent();
            this.ptTimestamp = outputPath.getName();
            buildRootPath = outputPath.toString();
        }
        this.ptVersion = runXmlFile.getRunXmlFile().getVersion();
        this.tag = runXmlFile.getRunXmlFile().getTag();
        Path successFile = new Path(buildRootPath, Constants.SUCCESS_TXT);
        this.localities = HdfsTools.forConfiguration(configuration).readLines(successFile.toString());
        splitLocality(localities);
    }

    public ArtifactsBean(String ptTimestamp, String ptVersion, String tag, List<String> localities) {
        super();
        this.ptTimestamp = ptTimestamp;
        this.ptVersion = ptVersion;
        this.tag = tag;
        this.localities = localities;
    }

    private void splitLocality(List<String> localities) {
        ptCountries = Maps.newHashMap();
        ptStates = Maps.newHashMap();
        for (String locality : localities) {
            String ptCountry;
            String ptState;
            if (localities.contains(Constants.PLUS)) {
                String[] split = locality.split(Constants.PLUSSPLITTER);
                ptCountry = split[0];
                ptState = split[1];
            } else {
                ptCountry = locality;
                ptState = Constants.BLANK;
            }
            ptCountries.put(locality, ptCountry);
            ptStates.put(locality, ptState);
        }
    }

    public Map<String, String> getPtCountries() {
        return ptCountries;
    }

    public Map<String, String> getPtStates() {
        return ptStates;
    }

    public String getPtTimestamp() {
        return ptTimestamp;
    }

    public String getPtVersion() {
        return ptVersion;
    }

    public String getTag() {
        return tag;
    }

    public List<String> getLocalities() {
        return localities;
    }

    public String getPtCountry(String locality) {
        return ptCountries.get(locality);
    }

    public String getPtState(String locality) {
        return ptStates.get(locality);
    }

}
