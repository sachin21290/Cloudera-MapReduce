package com.mapreduce.utility.reporting;

import static com.cloudera.crunch.types.avro.Avros.strings;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.FilterFn;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.PTable;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.Pipeline;
import com.cloudera.crunch.PipelineResult;
import com.cloudera.crunch.impl.mr.MRPipeline;
import com.cloudera.crunch.io.avro.AvroFileSource;
import com.cloudera.crunch.types.avro.Avros;
import com.cloudera.crunch.types.writable.Writables;
import com.mapreduce.archive.ArchivePlace;
import com.mapreduce.avro.trace.Trace;
import com.mapreduce.repository.RunXmlFile;
import com.mapreduce.HdfsTools;
import com.mapreduce.utility.reporting.keyfn.PlaceBeanKeyFn;
import com.mapreduce.utility.reporting.keyfn.LogKeyFn;
import com.mapreduce.utility.reporting.model.ArtifactsBean;
import com.mapreduce.utility.reporting.model.Constants;
import com.mapreduce.utility.reporting.model.MastercardBean;
import com.mapreduce.utility.reporting.props.PopulateParameters;
import com.mapreduce.utility.reporting.util.CSVWriterUtil;
import com.mapreduce.utility.reporting.util.ReportBeanBuilderUtil;

public class MapReduceReporting extends Configured implements Tool, Serializable {

    private static final String YYYYM_MDDHHMMSS = "YYYYMMddhhmmss";
    private static final long serialVersionUID = -4445070554322328601L;
    private ReportBeanBuilderUtil reportBeanBuilderUtil = new ReportBeanBuilderUtil();

    public int run(String[] args) throws Exception {
        PopulateParameters params = PopulateParameters.parseArguments(args);
        Pipeline pipeline = new MRPipeline(MapReduceReporting.class, getConf());
        return run(pipeline, params);
    }

    public int run(Pipeline pipeline, PopulateParameters params) throws IOException {
        pipeline.getConfiguration().setBoolean(Constants.MAPREDUCE_INPUT_FILEINPUTFORMAT_INPUT_DIR_RECURSIVE, true);

        List<String> runDescList = readRunXmlFileListFromHDFS(params.getRundescriptorList());
        PCollection<MastercardBean> output = createMasterCardRawOutput(pipeline, runDescList);
        String rawpathToken = getCurrentTimestamp();
        writeRawOutput(pipeline, output, rawpathToken);

        PCollection<MastercardBean> masterCardBeans = readRawOutput(pipeline, rawpathToken);
        writeReports(masterCardBeans, pipeline);

        doCleanup(rawpathToken);

        return 0;
    }

    public void writeReports(PCollection<MastercardBean> masterCardBeans, Pipeline pipeline) {
        List<String> uniqueTags = CSVWriterUtil.parseUniqueTagsAndWriteCSVReport(masterCardBeans);
        for (String tag : uniqueTags) {
            PCollection<MastercardBean> tagCollection = masterCardBeans.filter(getTagFilter(tag));
            CSVWriterUtil.writeCountrySpecificCSVReport(tagCollection, tag).materialize();
            CSVWriterUtil.writeCategorySpecificCSVReport(tagCollection, tag).materialize();
        }
        pipelineDone(pipeline);
    }

    private void writeRawOutput(Pipeline pipeline, PCollection<MastercardBean> output, String rawpathToken) {
        pipeline.writeTextFile(output, getRawOutputPath(rawpathToken));
        pipelineDone(pipeline);
    }

    private void pipelineDone(Pipeline pipeline) {
        PipelineResult pipelineResult = pipeline.done();
        if (pipelineResult != null && !pipelineResult.succeeded()) {
            throw new RuntimeException("Exception while writing output");
        }
    }

    private void doCleanup(String rawpathToken) {
        HdfsTools fileSystem = HdfsTools.forConfiguration(getConf());
        try {
            if (fileSystem.exists(getRawOutputPath(rawpathToken))) {
                fileSystem.deletePathRecursive(getRawOutputPath(rawpathToken));
            }
        } catch (IOException e) {
            throw new RuntimeException("Exception while cleanup " + e);
        }
    }

    private String getCurrentTimestamp() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(YYYYM_MDDHHMMSS);
        String dateAsString = simpleDateFormat.format(new Date());
        return dateAsString;
    }

    public PCollection<MastercardBean> readRawOutput(Pipeline pipeline, String rawpathToken) {
        PCollection<MastercardBean> masterCardBeans =
            pipeline.readTextFile(getRawOutputPath(rawpathToken)).parallelDo(
                new DoFn<String, MastercardBean>() {

                    private static final long serialVersionUID = -3220008709180454675L;

                    @Override
                    public void process(String input, Emitter<MastercardBean> emitter) {
                        MastercardBean mastercardBean = reportBeanBuilderUtil.buildMastercardBean(input);
                        emitter.emit(mastercardBean);
                    }
                }, Writables.records(MastercardBean.class));
        return masterCardBeans;
    }

    public String getRawOutputPath(String rawpathToken) {
        return Constants.REPORT_PATH + Constants.SLASH + Constants.RAWPATH + Constants.SLASH + rawpathToken;
    }

    private PCollection<MastercardBean> createMasterCardRawOutput(Pipeline pipeline, List<String> runDescList) throws IOException {
        PCollection<MastercardBean> output = null;
        ArtifactsBean artifactsBean = null;
        for (String runXmlFilePath : runDescList) {
            RunXmlFile runXmlFile = loadRunDescriptor(getConf(), runXmlFilePath);
            artifactsBean = new ArtifactsBean(runXmlFile, getConf());
            output = performJoin(pipeline, output, artifactsBean, runXmlFile);
        }
        return output;

    }

    @SuppressWarnings("unchecked")
    public PCollection<MastercardBean> performJoin(Pipeline pipeline, PCollection<MastercardBean> output, ArtifactsBean artifactsBean,
    	RunXmlFile runXmlFile) {
        for (String locality : artifactsBean.getLocalities()) {
            PTable<String, ArchivePlace> archivePlaces =
                processArchive(pipeline, runXmlFile.getArchivePlacesPath(locality, Constants.PRODUCT));
            PTable<String, Trace> traces =
                processTraces(pipeline, runXmlFile.getOutputTracePath(runXmlFile.isReleasable()) + Constants.SLASH + locality);
            PTable<String, Pair<ArchivePlace, Trace>> result = archivePlaces.join(traces);
            PCollection<MastercardBean> masterCardBeans =
                result.parallelDo(
                    buildMastercardBean(artifactsBean.getPtCountry(locality), artifactsBean.getPtState(locality),
                        artifactsBean.getPtVersion(), artifactsBean.getPtTimestamp(), artifactsBean.getTag()),
                    Writables.records(MastercardBean.class));
            output = (output == null) ? masterCardBeans : output.union(masterCardBeans);
        }
        return output;
    }

    private DoFn<Pair<String, Pair<ArchivePlace, Trace>>, MastercardBean> buildMastercardBean(final String ptCountry, final String ptState,
        final String ptVersion, final String ptTimestamp, final String tag) {
        return new DoFn<Pair<String, Pair<ArchivePlace, Trace>>, MastercardBean>() {

            private static final long serialVersionUID = -2063129133019652022L;

            @Override
            public void process(Pair<String, Pair<ArchivePlace, Trace>> input, Emitter<MastercardBean> emitter) {
                emitter.emit(reportBeanBuilderUtil.buildMastercardBean(input, ptCountry, ptState, ptVersion, ptTimestamp, tag));
            }

        };
    }

    public RunXmlFile loadRunDescriptor(Configuration hadoopConfig, String runXmlFilePath) {
        try {
            return RunXmlFile.loadFromFile(hadoopConfig, runXmlFilePath);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Invalid run descriptor: " + runXmlFilePath, e);
        } catch (IOException e) {
            throw new RuntimeException("Invalid run descriptor: " + runXmlFilePath, e);
        }
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        ToolRunner.run(new Configuration(), new MapReduceReporting(), args);
        System.out.println("Time taken: " + ((System.currentTimeMillis() - start) / 1000) + "s");
    }

    private PTable<String, ArchivePlace> processArchive(Pipeline pipeline, final String archivePath) {
        PCollection<ArchivePlace> archivePlaces =
            pipeline.read(new AvroFileSource<ArchivePlace>(new Path(archivePath), Avros.records(ArchivePlace.class)));
        return archivePlaces.parallelDo(new PlaceBeanKeyFn(),
            archivePlaces.getTypeFamily().tableOf(strings(), archivePlaces.getPType()));
    }

    private PTable<String, Trace> processTraces(Pipeline pipeline, final String archivePath) {
        PCollection<Trace> traces = pipeline.read(new AvroFileSource<Trace>(new Path(archivePath), Avros.records(Trace.class)));
        return traces.parallelDo(new LogKeyFn(), traces.getTypeFamily().tableOf(strings(), traces.getPType()));
    }

    public static FilterFn<MastercardBean> getTagFilter(final String tag) {
        return new FilterFn<MastercardBean>() {

            private static final long serialVersionUID = -5216734956929460547L;

            @Override
            public boolean accept(MastercardBean input) {
                if (input.getTag().equalsIgnoreCase(tag)) {
                    return true;
                }
                return false;
            }
        };
    }

    public List<String> readRunXmlFileListFromHDFS(String pathToRundescList) throws IOException {
        HdfsTools fileSystem = HdfsTools.forConfiguration(getConf());
        if (fileSystem.exists(pathToRundescList)) {
            return fileSystem.readLines(pathToRundescList);
        } else {
            throw new RuntimeException("RunXmlFile list file does not exist at given path: " + pathToRundescList);
        }
    }

}
