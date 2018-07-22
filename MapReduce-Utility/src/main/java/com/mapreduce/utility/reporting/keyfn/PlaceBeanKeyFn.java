package com.mapreduce.utility.reporting.keyfn;

import static com.mapreduce.util.CharSequenceUtil.asNonNullString;

import com.cloudera.crunch.DoFn;
import com.cloudera.crunch.Emitter;
import com.cloudera.crunch.Pair;
import com.mapreduce.avro.archive.ArchivePlace;
import com.mapreduce.avro.archive.ERR;
import com.mapreduce.avro.archive.NVL;
import com.mapreduce.avro.archive.POI;
import com.mapreduce.productized.archive.ArchivePlaces;
import com.mapreduce.utility.reporting.model.Constants;

public class PlaceBeanKeyFn extends DoFn<ArchivePlace, Pair<String, ArchivePlace>> {

    private static final long serialVersionUID = -3174116713617363522L;

    @Override
    public void process(ArchivePlace archivePlace, Emitter<Pair<String, ArchivePlace>> emitter) {
        POI poi = ArchivePlaces.getPoi(archivePlace);
        if (isValidPoi(archivePlace, poi) && isSupplierMMD(archivePlace)) {
            String deliveryPlaceId = getDeliveryPlaceId(archivePlace, poi);
            emitter.emit(Pair.of(deliveryPlaceId, archivePlace));
        }
    }

    private boolean isSupplierMMD(ArchivePlace archivePlace) {
        if (archivePlace.getSuppliers().get(0).getSupplierCode().toString().equalsIgnoreCase(Constants.MMD)) {
            return true;
        }
        return false;

    }

    private String getDeliveryPlaceId(ArchivePlace archivePlace, POI poi) {
        return archivePlace.getDeliveryPlaceIds().get(0).toString();
    }

    private boolean isValidPoi(ArchivePlace archivePlace, POI poi) {
        for (ERR error : archivePlace.getErr()) {
            if (error.getPoiUniqueId() != null && asNonNullString(error.getPoiUniqueId()).equals(asNonNullString(poi.getPoiUniqueId()))) {
                return false;
            }
        }
        for (NVL nvl : archivePlace.getInvalid()) {
            if (nvl.getBusId() != null && asNonNullString(nvl.getBusId()).equals(asNonNullString(poi.getPoiUniqueId()))) {
                return false;
            }
        }
        if (notEmpty(archivePlace.getIso3Country()) || notEmpty(poi.getCountry())) {
            return true;
        }
        return false;
    }

    private boolean notEmpty(CharSequence attribute) {
        return attribute != null && !Constants.BLANK.equals(attribute.toString());
    }
}
