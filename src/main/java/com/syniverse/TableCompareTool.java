package com.syniverse;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.FileSystemXmlApplicationContext;

public class TableCompareTool {
    private final static Logger log = LoggerFactory.getLogger(TableCompareTool.class);
    public static FileSystemXmlApplicationContext context;
    private int threads;
    private daoImpl dao;
    private int waitSeconds = 5;

    public static void main(String[] args) {
        try {
            context = new FileSystemXmlApplicationContext(new String[] { "spring-config.xml" });
        } catch (Exception e) {
            log.error("init spring error!", e);
            return;
        }
        TableCompareTool t = (TableCompareTool) context.getBean("mainClass");
        t.run();
    }

    public void run() {
        rangeGetter r;
        try {
            r = dao.getRange();
        } catch (Exception e) {
            log.error("get start key error!", e);
            return;
        }
        log.info("threads {}", threads);
        ArrayList<Thread> threadList = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            CompareTask t = new CompareTask(r, dao);
            t.start();
            threadList.add(t);
        }
        for (Thread t : threadList) {
            try {
                t.join();
            } catch (Exception e) {
                log.error("wait task to die error", e);
            }
        }
        log.info("wait {} seconds to bypass replica delay records", waitSeconds);
        dao.finalConfirm();
        log.info("done!");
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public daoImpl getDao() {
        return dao;
    }

    public void setDao(daoImpl dao) {
        this.dao = dao;
    }

    public int getWaitSeconds() {
        return waitSeconds;
    }

    public void setWaitSeconds(int waitSeconds) {
        this.waitSeconds = waitSeconds;
    }
}
