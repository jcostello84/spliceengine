package com.splicemachine.si2.coprocessors;

import com.splicemachine.constants.TxnConstants;
import com.splicemachine.si.utils.SIUtils;
import com.splicemachine.si2.data.hbase.HGet;
import com.splicemachine.si2.data.hbase.HScan;
import com.splicemachine.si2.data.hbase.HbRegion;
import com.splicemachine.si2.data.hbase.TransactorFactory;
import com.splicemachine.si2.filters.SIFilter;
import com.splicemachine.si2.si.api.Transactor;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.log4j.Logger;
import sun.reflect.generics.tree.ArrayTypeSignature;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class SIObserver extends BaseRegionObserver {
    private static Logger LOG = Logger.getLogger(SIObserver.class);
    protected HRegion region;
    private boolean tableEnvMatch = false;

    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        SpliceLogUtils.trace(LOG, "starting %s", SIObserver.class);
        region = ((RegionCoprocessorEnvironment) e).getRegion();
        tableEnvMatch = SIUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TxnConstants.TableEnv.USER_TABLE)
                || SIUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TxnConstants.TableEnv.USER_INDEX_TABLE)
                || SIUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TxnConstants.TableEnv.DERBY_SYS_TABLE);
        super.start(e);
    }

    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        SpliceLogUtils.trace(LOG, "stopping %s", SIObserver.class);
        super.stop(e);
    }

    @Override
    public void preGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<KeyValue> results) throws IOException {
        SpliceLogUtils.trace(LOG, "preGet %s", get);
        if (tableEnvMatch && shouldUseSI(new HGet(get))) {
            addSiFilterToGet(e, get);
        }
        super.preGet(e, get, results);
    }

    @Override
    public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s) throws IOException {
        SpliceLogUtils.trace(LOG, "preScannerOpen %s", scan);
        if (tableEnvMatch && shouldUseSI(new HScan(scan))) {
            addSiFilterToScan(e, scan);
        }
        return super.preScannerOpen(e, scan, s);
    }

    private boolean shouldUseSI(Object operation) {
        Transactor transactor = TransactorFactory.getTransactor();
        return transactor.isFilterNeeded(operation);
    }

    private void addSiFilterToGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get) throws IOException {
        Filter newFilter = makeSiFilter(e, get.getTimeRange(), get.getFilter());
        get.setFilter(newFilter);
    }

    private void addSiFilterToScan(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan) throws IOException {
        Filter newFilter = makeSiFilter(e, scan.getTimeRange(), scan.getFilter());
        scan.setFilter(newFilter);
    }

    private Filter makeSiFilter(ObserverContext<RegionCoprocessorEnvironment> e, TimeRange timeRange, Filter currentFilter) throws IOException {
        Transactor transactor = TransactorFactory.getTransactor();
        SIFilter siFilter = new SIFilter(transactor, timeRange.getMax() - 1, new HbRegion(e.getEnvironment().getRegion()));
        Filter newFilter;
        if (currentFilter != null) {
            newFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL, currentFilter, siFilter); // Wrap Existing Filters
        } else {
            newFilter = siFilter;
        }
        return newFilter;
    }
}
