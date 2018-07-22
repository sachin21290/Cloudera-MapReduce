package com.mapreduce.utility.reporting.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;

public class MastercardBean implements Serializable, Writable {

    /**
     * 
     */
    private static final long serialVersionUID = -3516793614565090833L;
    private String deliveryPlaceId;
    private String supplierCode;
    private String tag;
    private String gdfFeatureCode;
    private String gdfFeatureCodeDesc;
    private String subcategory;
    private String subcategoryDesc;
    private Boolean mandatory;
    private String externalIdentifier;
    private String isHighestRanked;
    private String releaseFlag;
    private String usedFlag;
    private String sourceExternalId;
    private String officialName;
    private String intakeFileNo;
    private String intakeFileName;
    private String ptCountry;
    private String ptState;
    private String ptVersion;
    private String ptTime;

    public MastercardBean() {
        this.deliveryPlaceId = "";
        this.supplierCode = "";
        this.tag = "";
        this.gdfFeatureCode = "";
        this.gdfFeatureCodeDesc = "";
        this.subcategory = "";
        this.subcategoryDesc = "";
        this.externalIdentifier = "";
        this.isHighestRanked = "";
        this.releaseFlag = "";
        this.usedFlag = "";
        this.sourceExternalId = "";
        this.officialName = "";
        this.intakeFileNo = "";
        this.intakeFileName = "";
        this.ptCountry = "";
        this.ptState = "";
        this.ptVersion = "";
        this.ptTime = "";
    }

    public String getDeliveryPlaceId() {
        return deliveryPlaceId;
    }

    public void setDeliveryPlaceId(String deliveryPlaceId) {
        this.deliveryPlaceId = deliveryPlaceId;
    }

    public void setSupplierCode(String supplierCode) {
        this.supplierCode = supplierCode;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public void setGdfFeatureCode(String gdfFeatureCode) {
        this.gdfFeatureCode = gdfFeatureCode;
    }

    public void setGdfFeatureCodeDesc(String gdfFeatureCodeDesc) {
        this.gdfFeatureCodeDesc = gdfFeatureCodeDesc;
    }

    public void setSubcategory(String subcategory) {
        this.subcategory = subcategory;
    }

    public void setSubcategoryDesc(String subcategoryDesc) {
        this.subcategoryDesc = subcategoryDesc;
    }

    public void setMandatory(Boolean mandatory) {
        this.mandatory = mandatory;
    }

    public void setExternalIdentifier(String externalIdentifier) {
        this.externalIdentifier = externalIdentifier;
    }

    public void setSourceExternalId(String sourceExternalId) {
        this.sourceExternalId = sourceExternalId;
    }

    public void setOfficialName(String officialName) {
        this.officialName = officialName;
    }

    public void setIntakeFileNo(String intakeFileNo) {
        this.intakeFileNo = intakeFileNo;
    }

    public void setIntakeFileName(String intakeFileName) {
        this.intakeFileName = intakeFileName;
    }

    public void setPtCountry(String ptCountry) {
        this.ptCountry = ptCountry;
    }

    public void setPtState(String ptState) {
        this.ptState = ptState;
    }

    public void setPtVersion(String ptVersion) {
        this.ptVersion = ptVersion;
    }

    public void setPtTime(String ptTime) {
        this.ptTime = ptTime;
    }

    public String getSupplierCode() {
        return supplierCode;
    }

    public String getTag() {
        return tag;
    }

    public String getGdfFeatureCode() {
        return gdfFeatureCode;
    }

    public String getGdfFeatureCodeDesc() {
        return gdfFeatureCodeDesc;
    }

    public String getSubcategory() {
        return subcategory;
    }

    public String getSubcategoryDesc() {
        return subcategoryDesc;
    }

    public Boolean getMandatory() {
        return mandatory;
    }

    public String getExternalIdentifier() {
        return externalIdentifier;
    }

    public String getSourceExternalId() {
        return sourceExternalId;
    }

    public String getOfficialName() {
        return officialName;
    }

    public String getIntakeFileNo() {
        return intakeFileNo;
    }

    public String getIntakeFileName() {
        return intakeFileName;
    }

    public String getPtCountry() {
        return ptCountry;
    }

    public String getPtState() {
        return ptState;
    }

    public String getPtVersion() {
        return ptVersion;
    }

    public String getPtTime() {
        return ptTime;
    }

    public String getIsHighestRanked() {
        return isHighestRanked;
    }

    public void setIsHighestRanked(String isHighestRanked) {
        this.isHighestRanked = isHighestRanked;
    }

    public String getReleaseFlag() {
        return releaseFlag;
    }

    public void setReleaseFlag(String releaseFlag) {
        this.releaseFlag = releaseFlag;
    }

    public String getUsedFlag() {
        return usedFlag;
    }

    public void setUsedFlag(String usedFlag) {
        this.usedFlag = usedFlag;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(tag)
            .append(Constants.PIPE).append(supplierCode)
            .append(Constants.PIPE).append(sourceExternalId)
            .append(Constants.PIPE).append(externalIdentifier)
            .append(Constants.PIPE).append(isHighestRanked)
            .append(Constants.PIPE).append(gdfFeatureCodeDesc)
            .append(Constants.PIPE).append(subcategoryDesc)
            .append(Constants.PIPE).append(mandatory)
            .append(Constants.PIPE).append(officialName)
            .append(Constants.PIPE).append(releaseFlag)
            .append(Constants.PIPE).append(usedFlag)
            .append(Constants.PIPE).append(ptCountry)
            .append(Constants.PIPE).append(ptState)
            .append(Constants.PIPE).append(ptVersion)
            .append(Constants.PIPE).append(ptTime);
        return builder.toString();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(tag);
        out.writeUTF(Constants.PIPE);
        out.writeUTF(supplierCode);
        out.writeUTF(Constants.PIPE);
        out.writeUTF(sourceExternalId);
        out.writeUTF(Constants.PIPE);
        out.writeUTF(externalIdentifier);
        out.writeUTF(Constants.PIPE);
        out.writeUTF(isHighestRanked);
        out.writeUTF(Constants.PIPE);
        out.writeUTF(gdfFeatureCodeDesc);
        out.writeUTF(Constants.PIPE);
        out.writeUTF(subcategoryDesc);
        out.writeUTF(Constants.PIPE);
        out.writeBoolean(mandatory);
        out.writeUTF(Constants.PIPE);
        out.writeUTF(officialName);
        out.writeUTF(Constants.PIPE);
        out.writeUTF(releaseFlag);
        out.writeUTF(Constants.PIPE);
        out.writeUTF(usedFlag);
        out.writeUTF(Constants.PIPE);
        out.writeUTF(ptCountry);
        out.writeUTF(Constants.PIPE);
        out.writeUTF(ptState);
        out.writeUTF(Constants.PIPE);
        out.writeUTF(ptVersion);
        out.writeUTF(Constants.PIPE);
        out.writeUTF(ptTime);
    }

    public void readFields(DataInput in) throws IOException {
        tag = in.readUTF();
        in.readUTF();
        supplierCode = in.readUTF();
        in.readUTF();
        sourceExternalId = in.readUTF();
        in.readUTF();
        externalIdentifier = in.readUTF();
        in.readUTF();
        isHighestRanked = in.readUTF();
        in.readUTF();
        gdfFeatureCodeDesc = in.readUTF();
        in.readUTF();
        subcategoryDesc = in.readUTF();
        in.readUTF();
        mandatory = in.readBoolean();
        in.readUTF();
        officialName = in.readUTF();
        in.readUTF();
        releaseFlag = in.readUTF();
        in.readUTF();
        usedFlag = in.readUTF();
        in.readUTF();
        ptCountry = in.readUTF();
        in.readUTF();
        ptState = in.readUTF();
        in.readUTF();
        ptVersion = in.readUTF();
        in.readUTF();
        ptTime = in.readUTF();
    }

}
