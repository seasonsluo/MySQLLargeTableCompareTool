package com.syniverse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompareTask extends Thread {
    private final static Logger log = LoggerFactory.getLogger(CompareTask.class);
    rangeGetter r;
    daoImpl dao;

    public CompareTask(rangeGetter r, daoImpl dao) {
        this.r = r;
        this.dao = dao;
    }

    public void run() {
        range rr = r.getNextStart();
        while (rr.isValid()) {
            log.info("{}% compared, now key {}", rr.getPercent(), rr.getStart());
            if (!dao.compareRange(rr.getStart())) {
                log.error("fail to compare start point {}", rr.getStart());
            }
            rr = r.getNextStart();
        }
    }
}
