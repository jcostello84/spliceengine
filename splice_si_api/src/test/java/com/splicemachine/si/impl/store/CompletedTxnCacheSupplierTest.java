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

package com.splicemachine.si.impl.store;

import com.splicemachine.concurrent.IncrementingClock;
import com.splicemachine.si.api.txn.*;
import com.splicemachine.si.impl.txn.WritableTxn;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static com.splicemachine.si.impl.TxnTestUtils.assertTxnsMatch;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 *         Date: 7/1/14
 */
@Category(ArchitectureIndependent.class)
public class CompletedTxnCacheSupplierTest{

    @Test
    public void testDoesNotCacheActiveTransactions() throws Exception{
        final AtomicLong al=new AtomicLong(0l);
        TxnLifecycleManager tc=mock(TxnLifecycleManager.class);
        when(tc.commit(anyLong())).thenAnswer(new Answer<Long>(){

            @Override
            public Long answer(InvocationOnMock invocationOnMock) throws Throwable{
                return al.incrementAndGet();
            }
        });
        Txn txn=new WritableTxn(1,1,null,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,tc,false,null);

        final boolean[] called=new boolean[]{false};
        TxnStore backStore=new TestingTxnStore(new IncrementingClock(),new TestingTimestampSource(),null,Long.MAX_VALUE){
            @Override
            public Txn getTransaction(long txnId,boolean getDestinationTables) throws IOException{
                Assert.assertFalse("Item should have been fed from cache!",called[0]);
                called[0]=true;
                return super.getTransaction(txnId,getDestinationTables);
            }
        };
        backStore.recordNewTransaction(txn);

        TxnSupplier store=new CompletedTxnCacheSupplier(backStore,10,16);


        //fetch the transaction from the underlying store
        Assert.assertFalse("Cache thinks it already has the item!",store.transactionCached(txn.getTxnId()));

        TxnView fromStore=store.getTransaction(txn.getTxnId());
        assertTxnsMatch("Transaction from store is not correct!",txn,fromStore);

        Assert.assertFalse("Cache does not think it is present!",store.transactionCached(txn.getTxnId()));
    }

    @Test
    public void testCachesRolledBackTransactions() throws Exception{
        final AtomicLong al=new AtomicLong(0l);
        TxnLifecycleManager tc=mock(TxnLifecycleManager.class);
        when(tc.commit(anyLong())).thenAnswer(new Answer<Long>(){

            @Override
            public Long answer(InvocationOnMock invocationOnMock) throws Throwable{
                return al.incrementAndGet();
            }
        });
        Txn txn=new WritableTxn(1,1,null,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,tc,false,null);
        txn.rollback();

        final boolean[] called=new boolean[]{false};
        TxnStore backStore=new TestingTxnStore(new IncrementingClock(),new TestingTimestampSource(),null,Long.MAX_VALUE){
            @Override
            public Txn getTransaction(long txnId,boolean getDestinationTables) throws IOException{
                Assert.assertFalse("Item should have been fed from cache!",called[0]);
                called[0]=true;
                return super.getTransaction(txnId,getDestinationTables);
            }
        };
        backStore.recordNewTransaction(txn);

        TxnSupplier store=new CompletedTxnCacheSupplier(backStore,10,16);


        //fetch the transaction from the underlying store
        Assert.assertFalse("Cache thinks it already has the item!",store.transactionCached(txn.getTxnId()));

        TxnView fromStore=store.getTransaction(txn.getTxnId());
        assertTxnsMatch("Transaction from store is not correct!",txn,fromStore);

        Assert.assertTrue("Cache does not think it is present!",store.transactionCached(txn.getTxnId()));

        TxnView fromCache=store.getTransaction(txn.getTxnId());
        assertTxnsMatch("Transaction from store is not correct!",txn,fromCache);
    }

    @Test
    public void testCachesCommittedTransactions() throws Exception{
        final AtomicLong al=new AtomicLong(0l);
        TxnLifecycleManager tc=mock(TxnLifecycleManager.class);
        when(tc.commit(anyLong())).thenAnswer(new Answer<Long>(){

            @Override
            public Long answer(InvocationOnMock invocationOnMock) throws Throwable{
                return al.incrementAndGet();
            }
        });
        Txn txn=new WritableTxn(1,1,null,Txn.IsolationLevel.SNAPSHOT_ISOLATION,Txn.ROOT_TRANSACTION,tc,false,null);
        txn.commit();

        final boolean[] called=new boolean[]{false};
        TxnStore backStore=new TestingTxnStore(new IncrementingClock(),new TestingTimestampSource(),null,Long.MAX_VALUE){
            @Override
            public Txn getTransaction(long txnId,boolean getDestinationTables) throws IOException{
                Assert.assertFalse("Item should have been fed from cache!",called[0]);
                called[0]=true;
                return super.getTransaction(txnId,getDestinationTables);
            }

            @Override
            public Txn getTransactionFromCache(long txnId){
                return null;
            }
        };
        backStore.recordNewTransaction(txn);

        TxnSupplier store=new CompletedTxnCacheSupplier(backStore,10,16);


        //fetch the transaction from the underlying store
        Assert.assertFalse("Cache thinks it already has the item!",store.transactionCached(txn.getTxnId()));

        TxnView fromStore=store.getTransaction(txn.getTxnId());
        assertTxnsMatch("Transaction from store is not correct!",txn,fromStore);

        Assert.assertTrue("Cache does not think it is present!",store.transactionCached(txn.getTxnId()));

        TxnView fromCache=store.getTransaction(txn.getTxnId());
        assertTxnsMatch("Transaction from store is not correct!",txn,fromCache);
    }
}
