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

package com.splicemachine.pipeline;

import java.io.IOException;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.derby.impl.sql.execute.index.IndexTransformer;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.callbuffer.CallBuffer;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.writehandler.RoutingWriteHandler;
import com.splicemachine.primitives.Bytes;
import org.apache.log4j.Logger;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Intercepts UPDATE/UPSERT/INSERT/DELETE mutations to a base table and sends corresponding mutations to the index table.
 *
 * @author Scott Fines
 *         Created on: 5/1/13
 */
public class IndexWriteHandler extends RoutingWriteHandler{
    private static final Logger LOG = Logger.getLogger(IndexWriteHandler.class);
    private final IndexTransformer transformer;
    private CallBuffer<KVPair> indexBuffer;
    private final int expectedWrites;
    private BitSet indexedColumns;

    public IndexWriteHandler(boolean keepState,
                             int expectedWrites,
                             IndexTransformer transformer){
        super(transformer.getIndexConglomBytes(),keepState);
        this.expectedWrites = expectedWrites;
        this.transformer = transformer;
        this.indexedColumns = transformer.gitIndexedCols();
    }

    @Override
    protected void doFlush(WriteContext ctx) throws Exception {
        if (indexBuffer != null && ! ctx.skipIndexWrites())
            indexBuffer.flushBuffer();
    }

    @Override
    public void flush(WriteContext ctx) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "flush");
        super.flush(ctx);
    }

    @Override
    public void close(WriteContext ctx) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "close");
        super.close(ctx);
    }

    @Override
    public void doClose(WriteContext ctx) throws Exception {
        if (indexBuffer != null)
            indexBuffer.close(); // Blocks
    }

    @Override
    protected boolean isHandledMutationType(KVPair.Type type) {
        return type == KVPair.Type.DELETE || type == KVPair.Type.CANCEL ||
            type == KVPair.Type.UPDATE || type == KVPair.Type.INSERT ||
            type == KVPair.Type.UPSERT;
    }

    @Override
    public boolean route(KVPair mutation,WriteContext ctx) {
        if (ctx.skipIndexWrites()) {
            return true;
        }
        if (!ensureBufferReader(mutation, ctx))
            return false;

        switch(mutation.getType()) {
            case INSERT:
                return createIndexRecord(mutation, ctx,null);
            case UPDATE:
                if (transformer.areIndexKeysModified(mutation, indexedColumns)) { // Do I need to update?
                    deleteIndexRecord(mutation, ctx);
                    return createIndexRecord(mutation, ctx,indexBuffer.lastElement());
                }
                return true; // No index columns modifies ignore...
            case UPSERT:
                deleteIndexRecord(mutation, ctx);
                return createIndexRecord(mutation, ctx,indexBuffer.lastElement());
            case DELETE:
                return deleteIndexRecord(mutation, ctx);
            case CANCEL:
                if (transformer.isUniqueIndex())
                    return true;
                throw new RuntimeException("Not Valid Execution Path");
            case EMPTY_COLUMN:
            default:
                throw new RuntimeException("Not Valid Execution Path");
        }
    }

    private boolean createIndexRecord(KVPair mutation, WriteContext ctx,KVPair deleteMutation) {
        try {
            boolean add=true;
            KVPair newIndex = transformer.translate(mutation);
            newIndex.setType(KVPair.Type.INSERT);
            if(deleteMutation!=null && newIndex.rowKeySlice().equals(deleteMutation.rowKeySlice())){
                /*
                 * DB-4165: When we do an update to the base table, that translates to a delete
                 * and then an insert in the index. For situations where we update the indexed fields
                 * to different values, this is fine because the delete will go to one HBase row, and the
                 * insert to another. However, if you update an indexed field by setting it to the same value
                 * (i.e. update foo set bar = bar), then the insert and the delete will end up going to the same
                 * location, and the result is an insert and a delete on the same row with the same transaction.
                 * The SI module treats this as a delete (because there is no anti-tombstone record at that location),
                 * and thus the row goes missing from the index; the end result is a corrupted index.
                 *
                 * To avoid this scenario, we check for whether the insert and the delete have the same row key. If
                 * they do, then we hijack the previous KVPair(the deleteMutation), and change it into an update mutation
                 * instead. That way, we still get the WWConflict detection, but we don't have an insert and a delete
                 * competing for the row results.
                 */
                deleteMutation.setValue(newIndex.getValue());
                deleteMutation.setType(KVPair.Type.UPDATE);
                newIndex = deleteMutation;
                add=false;
            }
            if(keepState) {
                this.routedToBaseMutationMap.put(newIndex, mutation);
            }
            if(add)
                indexBuffer.add(newIndex);
        } catch (Exception e) {
            fail(mutation,ctx,e);
            return false;
        }
        return true;
    }


    private boolean deleteIndexRecord(KVPair mutation, WriteContext ctx) {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "index delete with %s", mutation);

    	/*
         * To delete the correct index row, we do the following:
         *
         * 1. do a Get() on all the indexed columns of the main table
         * 2. transform the results into an index row (as if we were inserting it)
         * 3. issue a delete against the index table
         */
        try {
            KVPair indexDelete = transformer.createIndexDelete(mutation, ctx, indexedColumns);
            if (indexDelete == null) {
                // we can't find the old row, it may have been deleted already, but we'll have to update the
                // index anyway in the calling method
//                ctx.success(mutation);
                return false;
            }
            if(keepState)
                this.routedToBaseMutationMap.put(indexDelete,mutation);
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "performing index delete on row %s", Bytes.toHex(indexDelete.getRowKey()));
            ensureBufferReader(indexDelete, ctx);
            indexBuffer.add(indexDelete);
        } catch (Exception e) {
            fail(mutation,ctx,e);
            return false;
        }
        return true;
    }

    private boolean ensureBufferReader(KVPair mutation, WriteContext ctx) {
        if (indexBuffer == null) {
            try {
                indexBuffer = getRoutedWriteBuffer(ctx,expectedWrites);
            } catch (Exception e) {
                fail(mutation,ctx,e);
                return false;
            }
        }
        return true;
    }

}
