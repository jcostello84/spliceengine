/*
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.impl.ast;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.compile.Visitor;
import org.spark_project.guava.base.Predicate;
import org.spark_project.guava.base.Predicates;
import java.util.List;

/**
 * Build a CollectingVisitor and optionally wrap it in additional visitors.
 *
 * Use this when you want to visit all nodes in a tree and collect those that evaluate to true for a specified guava
 * predicate.
 */
public class CollectingVisitorBuilder<T> {

    private final CollectingVisitor<T> collector;
    private Visitor wrapped;

    private CollectingVisitorBuilder(Predicate<? super Visitable> nodePred,  Predicate<? super Visitable> parentPred) {
        collector = new CollectingVisitor<>(nodePred, parentPred);
        wrapped = collector;
    }

    /**
     * Terminal method in the fluent API.  Invokes the built Visitor and returns collected nodes.
     */
    public List<T> collect(Visitable node) throws StandardException {
        node.accept(wrapped);
        return collector.getCollected();
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // methods for wrapping CollectingVisitor to further refine visit path
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    public CollectingVisitorBuilder<T> skipping(Predicate<? super Visitable> p) {
        wrapped = new SkippingVisitor(wrapped, p);
        return this;
    }

    public CollectingVisitorBuilder<T> skipping(Class<?> clazz) {
        wrapped = new SkippingVisitor(wrapped, Predicates.instanceOf(clazz));
        return this;
    }

    public CollectingVisitorBuilder<T> until(Predicate<? super Visitable> p) {
        wrapped = new VisitUntilVisitor(wrapped, p);
        return this;
    }

    public CollectingVisitorBuilder<T> onAxis(Predicate<? super Visitable> p) {
        wrapped = new AxisVisitor(wrapped, p);
        return this;
    }


    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // static factory methods for building this builder.
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    public static <T> CollectingVisitorBuilder<T> forClass(Class<T> clazz) {
        return new CollectingVisitorBuilder<>(Predicates.instanceOf(clazz), Predicates.<Visitable>alwaysTrue());
    }

    public static <T> CollectingVisitorBuilder<T> forPredicate(Predicate<? super Visitable> pred) {
        return new CollectingVisitorBuilder<>(pred, Predicates.<Visitable>alwaysTrue());
    }
}
