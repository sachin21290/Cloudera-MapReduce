package com.mapreduce.utility.reporting.writer;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.mapreduce.HdfsTools;
import com.mapreduce.utility.reporting.model.Constants;
import com.mapreduce.utility.reporting.model.MastercardBean;

import au.com.bytecode.opencsv.CSVWriter;

public class DelimitedWriter implements Closeable {

    private CSVWriter writer;

    public void initializeWriter(String path, Configuration configuration, String[] headers, String tag)
        throws UnsupportedEncodingException,
        IOException {
        writer = new CSVWriter(
            new OutputStreamWriter(getHDFSOutputStream(path, Constants.MMD_DETAIL_REPORT, tag, configuration), Constants.UTF_8),
                Constants.SEPERATOR);
        writer.writeNext(headers);
    }

    private OutputStream getHDFSOutputStream(String path, String reportName, String tag, Configuration configuration) throws IOException {
        HdfsTools fileSystem = HdfsTools.forConfiguration(configuration);
        createDirectoryIfNotExists(fileSystem, path);
        OutputStream outputStream = fileSystem.getOutputStream(
            path + Constants.SLASH + tag.replaceAll(Constants.SLASH, Constants.UNDERSCORE) + Constants.UNDERSCORE + reportName
                + Constants.ECSV);
        return outputStream;
    }

    private void createDirectoryIfNotExists(HdfsTools fileSystem, String directory) throws IOException {
        if (!fileSystem.exists(new Path(directory))) {
            fileSystem.mkdirs(new Path(directory));
        }
    }

    public void writeNextRecord(MastercardBean mastercardBean)
        throws SQLException {
        String[] values = mastercardBean.toString().split(Constants.PIPESPLITTER);
        writer.writeNext(values);
    }

    public void close() {
        try {
            if (writer != null)
                writer.close();
        } catch (IOException e) {
            // Ignoring
        }
    }
}
