package com.mapreduce.utility.reporting.util;

import java.io.Serializable;
import java.util.List;

import com.cloudera.crunch.Pair;
import com.mapreduce.avro.archive.ArchiveName;
import com.mapreduce.avro.archive.ArchivePlace;
import com.mapreduce.avro.archive.POI;
import com.mapreduce.avro.trace.Trace;
import com.mapreduce.utility.reporting.model.Constants;
import com.mapreduce.utility.reporting.model.MastercardBean;

public class ReportBeanBuilderUtil implements Serializable {

    /**
     * 
     */
    private static final long serialVersionUID = -4746063270013783141L;

    public MastercardBean buildMastercardBean(Pair<String, Pair<ArchivePlace, Trace>> input, String ptCountry, String ptState,
        String ptVersion, String ptTime, String tag) {
        MastercardBean mastercardBean = new MastercardBean();
        mastercardBean.setDeliveryPlaceId(input.first().toString());
        if (isNonEmpty(input.second().first().getSuppliers())) {
            mastercardBean.setSupplierCode(toString(input.second().first().getSuppliers().get(0).getSupplierCode()));
        }
        mastercardBean.setTag(tag);
        List<POI> pois = input.second().first().getPois();
        if (isNonEmpty(pois)) {
            mastercardBean.setGdfFeatureCode(toString(pois.get(0).getGdfFeatureCode()));
            mastercardBean.setGdfFeatureCodeDesc(toString(pois.get(0).getGdfFeatureCodeDesc()));
            mastercardBean.setSubcategory(toString(pois.get(0).getSubcategory()));
            mastercardBean.setSubcategoryDesc(toString(pois.get(0).getSubcategoryDesc()));
            mastercardBean.setMandatory(pois.get(0).getIsMandatory());
            mastercardBean.setExternalIdentifier(toString(pois.get(0).getExternalIdentifier()));
            List<ArchiveName> officialNames = pois.get(0).getOfficialNames();
            if (isNonEmpty(officialNames)) {
                mastercardBean.setOfficialName(toString(officialNames.get(0).getName()));
            }
        }
        List<CharSequence> originObjectIds = input.second().second().getOriginObjectIds();
        mastercardBean.setSourceExternalId(parseSourceExternalId(originObjectIds));
        mastercardBean.setIntakeFileNo(parseIntakeFileNo(originObjectIds));
        mastercardBean.setIntakeFileName(parseIntakeFileName(originObjectIds));
        mastercardBean.setPtCountry(ptCountry);
        mastercardBean.setPtState(ptState);
        mastercardBean.setPtVersion(ptVersion);
        mastercardBean.setPtTime(ptTime);
        mastercardBean.setIsHighestRanked(Constants.TRUE);
        mastercardBean.setReleaseFlag(Constants.BLANK);
        mastercardBean.setUsedFlag(Constants.Y);

        return mastercardBean;
    }

    private boolean isNonEmpty(List<? extends Object> input) {
        return input.size() >= 1;
    }

    private String parseIntakeFileName(List<CharSequence> originObjectIds) {
        if (originObjectIds.size() >= 3) {
            String deliveryInfo = originObjectIds.get(1).toString();
            return deliveryInfo.substring(deliveryInfo.lastIndexOf(Constants.HYPHEN) + 1);
        }
        return null;
    }

    private String parseIntakeFileNo(List<CharSequence> originObjectIds) {
        if (originObjectIds.size() >= 3) {
            return originObjectIds.get(0).toString();
        }
        return null;
    }

    private String parseSourceExternalId(List<CharSequence> originObjectIds) {
        if (originObjectIds.size() >= 3) {
            return originObjectIds.get(2).toString();
        }
        return null;
    }

    private String toString(CharSequence value) {
        return value != null ? value.toString() : Constants.BLANK;
    }

    public MastercardBean buildMastercardBean(String input) {
        String[] columnValues = input.split(Constants.PIPESPLITTER);
        int index = 0;
        MastercardBean mastercardBean = new MastercardBean();
        mastercardBean.setTag(columnValues[index++]);
        mastercardBean.setSupplierCode(columnValues[index++]);
        mastercardBean.setSourceExternalId(columnValues[index++]);
        mastercardBean.setExternalIdentifier(columnValues[index++]);
        mastercardBean.setIsHighestRanked(columnValues[index++]);
        mastercardBean.setGdfFeatureCodeDesc(columnValues[index++]);
        mastercardBean.setSubcategoryDesc(columnValues[index++]);
        mastercardBean.setMandatory(toBoolean(columnValues[index++]));
        mastercardBean.setOfficialName(columnValues[index++]);
        mastercardBean.setReleaseFlag(columnValues[index++]);
        mastercardBean.setUsedFlag(columnValues[index++]);
        mastercardBean.setPtCountry(columnValues[index++]);
        mastercardBean.setPtState(columnValues[index++]);
        mastercardBean.setPtVersion(columnValues[index++]);
        mastercardBean.setPtTime(columnValues[index++]);
        return mastercardBean;
    }

    private Boolean toBoolean(String value) {
        if (value.equalsIgnoreCase(Constants.TRUE)) {
            return true;
        } else {
            return false;
        }
    }
}
