package com.gizemgozde.loganalysis.loganalysisconsumer;

import java.io.Serializable;
import java.util.Date;

public class CityLogCountStatistics implements Serializable {

    long logCount;
    Date time;

    public CityLogCountStatistics() {
    }

    public CityLogCountStatistics add(String jobRow) {

        logCount += 1;
        time = new Date();
        return this;
    }

    public CityLogCountStatistics computeAvgTime(){
        return this;
    }
}
