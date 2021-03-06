/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.utils;

import org.spark_project.guava.base.Throwables;
import org.spark_project.guava.collect.Lists;
import org.spark_project.guava.collect.Maps;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.impl.driver.SIDriver;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Utility to conveniently reporting errors that are thrown during processing.
 *
 * @author Scott Fines
 * Created on: 8/15/13
 */
public class ErrorReporter implements ErrorReport{
    private static final ErrorReporter INSTANCE = new ErrorReporter();

    private final AtomicLong totalErrors = new AtomicLong(0l);
    private final AtomicLong totalRuntimeErrors = new AtomicLong(0l);
    private final AtomicLong totalStandardErrors = new AtomicLong(0l);
    private final AtomicLong totalDoNotRetryIOErrors = new AtomicLong(0l);
    private final AtomicLong totalIOExceptions = new AtomicLong(0l);
    private final AtomicLong totalExecutionErrors = new AtomicLong(0l);

    private final BlockingQueue<ErrorInfo> mostRecentErrors = new ArrayBlockingQueue<ErrorInfo>(100,true);

    private ErrorReporter(){ }

    public static ErrorReporter get(){
        return INSTANCE;
    }

    public void reportError(Class reportingClass, Throwable error){
        ErrorInfo info = new ErrorInfo(error,reportingClass);
        totalErrors.incrementAndGet();
        if(info.isRuntimeException())
            totalRuntimeErrors.incrementAndGet();
        if(info.isStandardException())
            totalStandardErrors.incrementAndGet();
        if(info.isDoNotRetryIOException())
            totalDoNotRetryIOErrors.incrementAndGet();
        if(info.isIOException())
            totalIOExceptions.incrementAndGet();
        if(info.isExecutionException())
            totalExecutionErrors.incrementAndGet();

        boolean success = true;
        do{
            if(!success)
                mostRecentErrors.poll(); //remove an entry

            success = mostRecentErrors.offer(info);
        }while(!success);

    }

    @Override
    public List<String> getRecentThrowingClassNames() {
        List<String> mostRecentThrowingClassNames = Lists.newArrayListWithCapacity(mostRecentErrors.size());
        for(ErrorInfo errorInfo:mostRecentErrors){
            mostRecentThrowingClassNames.add(errorInfo.getThrowingClass());
        }
        return mostRecentThrowingClassNames;
    }

    @Override
    public List<String> getRecentReportingClassNames() {
        List<String> mostRecentReportingClassNames = Lists.newArrayListWithCapacity(mostRecentErrors.size());
        for(ErrorInfo errorInfo:mostRecentErrors){
            mostRecentReportingClassNames.add(errorInfo.getReportingClass().getCanonicalName());
        }
        return mostRecentReportingClassNames;
    }

    @Override
    public Map<String, Long> getMostRecentErrors() {
        Map<String,Long> mostRecentExceptions = Maps.newIdentityHashMap();
        for(ErrorInfo info: mostRecentErrors){
            mostRecentExceptions.put(info.getError().getMessage(),info.getTimestamp());
        }
        return mostRecentExceptions;
    }

    @Override
    public long getTotalErrors() {
        return totalErrors.get();
    }

    @Override
    public long getTotalIOExceptions() {
        return totalIOExceptions.get();
    }

    @Override
    public long getTotalDoNotRetryIOExceptions() {
        return totalDoNotRetryIOErrors.get();
    }

    @Override
    public long getTotalStandardExceptions() {
        return totalStandardErrors.get();
    }

    @Override
    public long getTotalExecutionExceptions() {
        return totalExecutionErrors.get();
    }

    @Override
    public long getTotalRuntimeExceptions() {
        return totalRuntimeErrors.get();
    }

    private static class ErrorInfo{
        private final Throwable error;
        private final Class<?> reportingClass;
        private final long timestamp;
        private final ExceptionFactory exceptionFactory;

        private ErrorInfo(Throwable error, Class<?> reportingClass) {
            this.error = error;
            this.reportingClass = reportingClass;
            this.timestamp = System.currentTimeMillis();
            this.exceptionFactory = SIDriver.driver().getExceptionFactory();
        }

        public boolean isStandardException(){
            return error instanceof StandardException;
        }

        public boolean isDoNotRetryIOException(){
            return exceptionFactory.allowsRetry(error);
        }

        public boolean isIOException(){
            return error instanceof IOException;
        }

        public boolean isRuntimeException(){
            return error instanceof RuntimeException;
        }

        public boolean isExecutionException(){
            return error instanceof ExecutionException;
        }

        public Class<?> getReportingClass(){
            return reportingClass;
        }

        public Throwable getError(){
            return error;
        }

        public long getTimestamp(){
            return timestamp;
        }

        public String getThrowingClass(){
            Throwable e = Throwables.getRootCause(error);
            StackTraceElement[] stack = e.getStackTrace();
            if(stack!=null&&stack.length>0){
                return stack[stack.length-1].getClassName();
            }
            return "unknown";
        }
    }


}
