package com.mapreduce.utility.reporting.util;

import static com.cloudera.crunch.types.avro.Avros.strings;

import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.PCollection;
import com.cloudera.crunch.Pair;
import com.cloudera.crunch.types.writable.Writables;
import com.google.common.collect.Lists;
import com.mapreduce.utility.reporting.model.Constants;
import com.mapreduce.utility.reporting.model.MastercardBean;
import com.mapreduce.utility.reporting.writer.DelimitedWriter;

public class CSVWriterUtil {

    public static List<String> parseUniqueTagsAndWriteCSVReport(PCollection<MastercardBean> output) {
        PCollection<String> pTagCollection =
            output.parallelDo(tagKeyDoFn(), output.getTypeFamily().tableOf(Writables.strings(), Writables.records(MastercardBean.class)))
                .groupByKey()
                .parallelDo(new DoFn<Pair<String, Iterable<MastercardBean>>, String>() {

                    private static final long serialVersionUID = 6337253180142974806L;

                    @Override
                    public void process(Pair<String, Iterable<MastercardBean>> input, Emitter<String> emitter) {
                        writeCSVReport(new DelimitedWriter(), getTagSpecificReportPath(input.first()), input.second(), getConfiguration(),
                            input.first());
                        emitter.emit(input.first());
                    }
                }, strings());

        return parseTags(pTagCollection);
    }

    private static List<String> parseTags(PCollection<String> pTagCollection) {
        List<String> uniqueTags = Lists.newArrayList();
        for (String record : pTagCollection.materialize()) {
            uniqueTags.add(record);
        }
        return uniqueTags;
    }

    public static PCollection<String> writeCountrySpecificCSVReport(PCollection<MastercardBean> output, final String tag) {
        return output
            .parallelDo(countryKeyDoFn(), output.getTypeFamily().tableOf(Writables.strings(), Writables.records(MastercardBean.class)))
            .groupByKey()
            .parallelDo(new DoFn<Pair<String, Iterable<MastercardBean>>, String>() {

                private static final long serialVersionUID = 246490837074923446L;

                @Override
                public void process(Pair<String, Iterable<MastercardBean>> input, Emitter<String> emitter) {
                    writeCSVReport(new DelimitedWriter(), getCountrySpecificReportPath(tag, input.first()), input.second(),
                        getConfiguration(), tag);
                    emitter.emit(input.first());
                }
            }, strings());
    }

    public static PCollection<String> writeCategorySpecificCSVReport(PCollection<MastercardBean> output, final String tag) {
        return output
            .parallelDo(categoryKeyDoFn(), output.getTypeFamily().tableOf(Writables.strings(), Writables.records(MastercardBean.class)))
            .groupByKey()
            .parallelDo(new DoFn<Pair<String, Iterable<MastercardBean>>, String>() {

                private static final long serialVersionUID = -6931207869069939807L;

                @Override
                public void process(Pair<String, Iterable<MastercardBean>> input, Emitter<String> emitter) {
                    writeCSVReport(new DelimitedWriter(), getCategorySpecificReportPath(tag, input.first()), input.second(),
                        getConfiguration(), tag);
                    emitter.emit(input.first());
                }
            }, strings());
    }

    private static DoFn<MastercardBean, Pair<String, MastercardBean>> tagKeyDoFn() {
        return new DoFn<MastercardBean, Pair<String, MastercardBean>>() {

            private static final long serialVersionUID = 5479711344078160354L;

            @Override
            public void process(MastercardBean input, Emitter<Pair<String, MastercardBean>> emitter) {
                String tag = input.getTag();
                emitter.emit(Pair.of(tag, input));
            }

        };
    }

    private static DoFn<MastercardBean, Pair<String, MastercardBean>> countryKeyDoFn() {
        return new DoFn<MastercardBean, Pair<String, MastercardBean>>() {

            private static final long serialVersionUID = 5479711344078160234L;

            @Override
            public void process(MastercardBean input, Emitter<Pair<String, MastercardBean>> emitter) {
                String country = input.getPtCountry();
                emitter.emit(Pair.of(country, input));
            }

        };
    }

    private static DoFn<MastercardBean, Pair<String, MastercardBean>> categoryKeyDoFn() {
        return new DoFn<MastercardBean, Pair<String, MastercardBean>>() {

            private static final long serialVersionUID = -6495657282547105679L;

            @Override
            public void process(MastercardBean input, Emitter<Pair<String, MastercardBean>> emitter) {
                String category = input.getGdfFeatureCodeDesc();
                emitter.emit(Pair.of(category, input));
            }

        };
    }

    public static void writeCSVReport(DelimitedWriter writer, String path, Iterable<MastercardBean> iterable, Configuration configuration,
        String tag) {
        try {
            writer.initializeWriter(path, configuration, Constants.HEADERS, tag);
            for (MastercardBean mastercardBean : iterable) {
                writer.writeNextRecord(mastercardBean);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error while writing to file: " + path, e);
        } finally {
            IOUtils.closeQuietly(writer);
        }
    }

    private static String getCategorySpecificReportPath(String tag, String category) {
        if (category == null || category.trim().equals(Constants.BLANK)) {
            category = Constants.NONE;
        } else {
            category = category.replaceAll(Constants.SLASH, Constants.UNDERSCORE);
        }
        return getTagSpecificReportPath(tag) + Constants.SLASH + Constants.GDF_FEATURE_CODE + Constants.SLASH + category;
    }

    private static String getCountrySpecificReportPath(String tag, String country) {
        if (country == null || country.trim().equals(Constants.BLANK)) {
            country = Constants.NONE;
        } else {
            country = country.replaceAll(Constants.SLASH, Constants.UNDERSCORE);
        }
        return getTagSpecificReportPath(tag) + Constants.SLASH + Constants.PT_COUNTRY + Constants.SLASH + country;
    }

    private static String getTagSpecificReportPath(String tag) {
        if (tag == null || tag.trim().equals(Constants.BLANK)) {
            tag = Constants.NONE;
        } else {
            tag = tag.replaceAll(Constants.SLASH, Constants.UNDERSCORE);
        }
        return Constants.REPORT_PATH + Constants.SLASH + tag;
    }

}
