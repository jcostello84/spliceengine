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

package com.splicemachine.dbTesting.unitTests.store;

import com.splicemachine.db.iapi.store.access.*;

// used by unit tests, that needs  to simulate
//  ColumnOrdering  data type parameter from the language layer.

public class T_ColumnOrderingImpl implements ColumnOrdering
{
	int columnId;
	boolean isAscending;

	public	T_ColumnOrderingImpl(int columnId, boolean isAscending)
	{
		this.columnId = columnId;
		this.isAscending = isAscending;
	}

	/*
	 * Methods of ColumnOrdering
	 */

	/**
	@see ColumnOrdering#getColumnId
	**/
	public int getColumnId()
	{
		return this.columnId;
	}

	/**
	@see ColumnOrdering#getIsAscending
	**/
	public boolean getIsAscending()
	{
		return this.isAscending;
	}

	/**
	@see ColumnOrdering#getIsNullsOrderedLow
	**/
	public boolean getIsNullsOrderedLow()
	{
		return false;
	}
}

