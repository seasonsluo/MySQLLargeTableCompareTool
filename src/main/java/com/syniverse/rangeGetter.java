package com.syniverse;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class rangeGetter {
    private final static Logger log = LoggerFactory.getLogger(rangeGetter.class);
    ArrayBlockingQueue<Long> q = new ArrayBlockingQueue<>(100);
    private long end = 0;
    private int step = 1000;
    private volatile boolean isEnd = false;
    private AtomicLong count = new AtomicLong(0);

    public range getNextStart() {
        range r = new range();
        long process = 0;
        try {
            Long ret = null;
            while (!isEnd) {
                ret = q.poll(1, TimeUnit.SECONDS);
                if (ret != null) {
                    r.setStart(ret);
                    break;
                }
            }
            if (ret == null) {
                return r;
            } else {
                process = count.addAndGet(step);
            }
        } catch (InterruptedException e) {
            log.error("Can not get start point!", e);
            return r;
        }
        int percent = (int) (new Float(process) / new Float(end) * 100);
        percent = percent > 100 ? 100 : percent;
        r.setPercent(percent);
        r.setValid(true);
        return r;
    }

    public void addPoint(long start) {
        try {
            q.put(start);
        } catch (Exception e) {
            log.error("Can not put queue!", e);
        }
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public boolean isEnd() {
        return isEnd;
    }

    public void setEnd(boolean isEnd) {
        this.isEnd = isEnd;
    }

    public int getStep() {
        return step;
    }

    public void setStep(int step) {
        this.step = step;
    }
}
