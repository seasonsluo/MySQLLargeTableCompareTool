package com.syniverse;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

public class daoImpl {
    private final static Logger log = LoggerFactory.getLogger(daoImpl.class);
    private final static Logger difflog = LoggerFactory.getLogger("difflog");
    private final static Logger masterlog = LoggerFactory.getLogger("masterlog");
    private final static Logger slavelog = LoggerFactory.getLogger("slavelog");
    private JdbcTemplate master;
    private JdbcTemplate slave;
    private String tableName;
    private String keyColumn;
    private int rows;
    private String compareColumns;
    private rangeGetter r;
    private long startPoint = 0;
    private Set<Long> differentKeys = new ConcurrentSkipListSet<>();
    private long maxFoundLimit = 100000;

    public JdbcTemplate getMaster() {
        return master;
    }

    public void setMaster(JdbcTemplate master) {
        this.master = master;
    }

    public JdbcTemplate getSlave() {
        return slave;
    }

    public void setSlave(JdbcTemplate slave) {
        this.slave = slave;
    }

    public rangeGetter getRange() {
        String sql = "SELECT MIN(" + getKeyColumn() + "), COUNT(" + getKeyColumn() + ") FROM " + getTableName();
        r = new rangeGetter();
        r.setStep(getRows());
        SqlRowSet rs = master.queryForRowSet(sql);
        long start = 0;
        if (rs.next()) {
            start = rs.getLong(1);
            r.setEnd(rs.getLong(2));
        }
        rs = slave.queryForRowSet(sql);
        if (rs.next()) {
            if (rs.getLong(1) < start) {
                start = rs.getLong(1);
            }
            if (rs.getLong(2) > r.getEnd()) {
                r.setEnd(rs.getLong(2));
            }
        }
        if (startPoint != 0) {
            r.addPoint(startPoint);
        } else {
            r.addPoint(start);
        }
        return r;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getKeyColumn() {
        return keyColumn;
    }

    public void setKeyColumn(String keyColumn) {
        this.keyColumn = keyColumn;
    }

    public int getRows() {
        return rows;
    }

    public void setRows(int rows) {
        this.rows = rows;
    }

    public String getCompareColumns() {
        return compareColumns;
    }

    public void setCompareColumns(String compareColumns) {
        this.compareColumns = compareColumns;
    }

    public void finalConfirm() {
        int i = 0;
        StringBuilder sb = new StringBuilder();
        log.info("final confirm keys size {}", differentKeys.size());
        for (Long key : differentKeys) {
            if (i++ == 0) {
                sb.append(key);
            } else {
                sb.append(",").append(key);
                if (i == 100) {
                    i = 0;
                    compareKeys(sb.toString());
                    sb.setLength(0);
                }
            }
        }
        if (sb.length() > 0) {
            compareKeys(sb.toString());
        }
    }

    public void compareKeys(String keys) {
        String sql = getKeysSql(keys);
        TreeMap<Long, List<String>> masterMap = null;
        TreeMap<Long, List<String>> slaveMap = null;
        try {
            masterMap = getData(sql, master);
        } catch (Exception e) {
            try {
                masterMap = getData(sql, master);
            } catch (Exception e1) {
                log.error("Get master data error!", e1);
                return;
            }
        }
        try {
            slaveMap = getData(sql, slave);
        } catch (Exception e) {
            try {
                slaveMap = getData(sql, slave);
            } catch (Exception e1) {
                log.error("Get slave data error!", e1);
                return;
            }
        }
        Iterator<Entry<Long, List<String>>> mit = masterMap.entrySet().iterator();
        while (mit.hasNext()) {
            Entry<Long, List<String>> entry = mit.next();
            Long key = entry.getKey();
            List<String> values = entry.getValue();
            List<String> slaveValues = slaveMap.get(key);
            if (slaveValues == null) {
                // no key in slave, master only
                printMasterOnlyValue(key, values);
                mit.remove();
                continue;
            }
            Iterator<String> vit = values.iterator();
            while (vit.hasNext()) {
                String value = vit.next();
                if (slaveValues.contains(value)) {
                    // same record, remove
                    vit.remove();
                    slaveValues.remove(value);
                }
            }
            if (values.isEmpty()) {
                if (!slaveValues.isEmpty()) {
                    // slave only record
                    printSlaveOnlyValue(key, slaveValues);
                }
            } else {
                if (slaveValues.isEmpty()) {
                    // master only record
                    printMasterOnlyValue(key, values);
                } else {
                    // different record
                    printDifferentValue(key, values, slaveValues);
                }
            }
            mit.remove();
            slaveMap.remove(key);
        }
        for (Entry<Long, List<String>> entry : slaveMap.entrySet()) {
            printSlaveOnlyValue(entry.getKey(), entry.getValue());
        }
    }

    public boolean compareRange(long start) {
        String sql = getMasterSql(start, getRows());
        TreeMap<Long, List<String>> masterMap = null;
        TreeMap<Long, List<String>> slaveMap = null;
        try {
            masterMap = getData(sql, master);
        } catch (Exception e) {
            try {
                masterMap = getData(sql, master);
            } catch (Exception e1) {
                log.error("Get master data error!", e1);
                return false;
            }
        }
        if (masterMap.isEmpty()) {
            sql = getMasterSql(start, getRows());
            try {
                slaveMap = getData(sql, slave);
            } catch (Exception e) {
                try {
                    slaveMap = getData(sql, slave);
                } catch (Exception e1) {
                    log.error("Get slave data error!", e1);
                    return false;
                }
            }
            if (!slaveMap.isEmpty()) {
                long end = slaveMap.lastKey();
                if (slaveMap.size() == getRows()) {
                    r.addPoint(end + 1);
                } else {
                    r.setEnd(true);
                }
            } else {
                r.setEnd(true);
            }
        } else {
            long end = masterMap.lastKey();
            r.addPoint(end + 1);
            sql = getSlaveSql(start, end);
            try {
                slaveMap = getData(sql, slave);
            } catch (Exception e) {
                try {
                    slaveMap = getData(sql, slave);
                } catch (Exception e1) {
                    log.error("Get slave data error!", e1);
                    return false;
                }
            }
        }
        Iterator<Entry<Long, List<String>>> mit = masterMap.entrySet().iterator();
        while (mit.hasNext()) {
            Entry<Long, List<String>> entry = mit.next();
            Long key = entry.getKey();
            List<String> values = entry.getValue();
            List<String> slaveValues = slaveMap.get(key);
            if (slaveValues == null) {
                differentKeys.add(key);
                mit.remove();
                continue;
            }
            Iterator<String> vit = values.iterator();
            while (vit.hasNext()) {
                String value = vit.next();
                if (slaveValues.contains(value)) {
                    // same record, remove
                    vit.remove();
                    slaveValues.remove(value);
                }
            }
            if (values.isEmpty()) {
                if (!slaveValues.isEmpty()) {
                    // slave only record
                    differentKeys.add(key);
                }
            } else {
                if (slaveValues.isEmpty()) {
                    // master only record
                    differentKeys.add(key);
                } else {
                    // different record
                    differentKeys.add(key);
                }
            }
            mit.remove();
            slaveMap.remove(key);
        }
        for (Entry<Long, List<String>> entry : slaveMap.entrySet()) {
            differentKeys.add(entry.getKey());
        }
        if (differentKeys.size() >= getMaxFoundLimit()) {
            log.info("reach max limit differences: {}, stop each threads gracefully.", differentKeys.size());
            r.setEnd(true);
        }
        return true;
    }

    private void printMasterOnlyValue(Long key, List<String> values) {
        for (String value : values) {
            masterlog.info("MasterOnly: {},{}", key, value);
        }
    }

    private void printSlaveOnlyValue(Long key, List<String> values) {
        for (String value : values) {
            slavelog.info("SlaveOnly: {},{}", key, value);
        }
    }

    private void printDifferentValue(Long key, List<String> values, List<String> slaveValues) {
        StringBuilder sb = new StringBuilder();
        sb.append("master:");
        for (String v : values) {
            sb.append(" [").append(v).append("]");
        }
        sb.append(" / slave:");
        for (String v : slaveValues) {
            sb.append(" [").append(v).append("]");
        }
        difflog.info("Different: {} / {}", key, sb.toString());
    }

    private TreeMap<Long, List<String>> getData(String sql, JdbcTemplate jt) {
        TreeMap<Long, List<String>> map = new TreeMap<>();
        SqlRowSet rs = jt.queryForRowSet(sql);
        while (rs.next()) {
            Long keyV = rs.getLong(getKeyColumn());
            String otherV = rs.getString("other");
            List<String> l = map.get(keyV);
            if (l == null) {
                l = new ArrayList<>();
                map.put(keyV, l);
            }
            l.add(otherV);
        }
        return map;
    }

    private String getMasterSql(long start, long step) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(getKeyColumn());
        if (getCompareColumns() != null && !getCompareColumns().isEmpty()) {
            sb.append(",CONCAT_WS(',',").append(getCompareColumns()).append(") as other");
        }
        sb.append(" FROM ").append(getTableName())
                .append(" WHERE ").append(getKeyColumn()).append(">=").append(start)
                .append(" ORDER BY ").append(getKeyColumn())
                .append(" LIMIT ").append(step);
        String ret = sb.toString();
        log.debug(ret);
        return ret;
    }

    private String getSlaveSql(long start, long end) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(getKeyColumn());
        if (getCompareColumns() != null && !getCompareColumns().isEmpty()) {
            sb.append(",CONCAT_WS(',',").append(getCompareColumns()).append(") as other");
        }
        sb.append(" FROM ").append(getTableName())
                .append(" WHERE ").append(getKeyColumn()).append(">=").append(start)
                .append(" AND ").append(getKeyColumn()).append("<=").append(end);
        String ret = sb.toString();
        log.debug(ret);
        return ret;
    }

    private String getKeysSql(String keys) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ").append(getKeyColumn());
        if (getCompareColumns() != null && !getCompareColumns().isEmpty()) {
            sb.append(",CONCAT_WS(',',").append(getCompareColumns()).append(") as other");
        }
        sb.append(" FROM ").append(getTableName())
                .append(" WHERE ").append(getKeyColumn()).append(" IN (").append(keys).append(")");
        String ret = sb.toString();
        log.debug(ret);
        return ret;
    }

    public long getStartPoint() {
        return startPoint;
    }

    public void setStartPoint(long startPoint) {
        this.startPoint = startPoint;
    }

    public long getMaxFoundLimit() {
        return maxFoundLimit;
    }

    public void setMaxFoundLimit(long maxFoundLimit) {
        this.maxFoundLimit = maxFoundLimit;
    }

}
