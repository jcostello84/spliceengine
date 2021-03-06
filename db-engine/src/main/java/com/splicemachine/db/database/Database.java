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

package com.splicemachine.db.database;

/*
  The com.splicemachine.db.iapi.db.Database interface is all the externally
  available methods on a database.  These are methods that might be called from 
  an SQL-J CALL statement. 

  The Javadoc comment that follows is for external consumption.
*/


import com.splicemachine.db.catalog.UUID;

import java.sql.SQLException;
import java.util.Locale;

/**
 * The Database interface provides control over a database
 * (that is, the stored data and the files the data are stored in),
 * operations on the database such as  backup and recovery,
 * and all other things that are associated with the database itself.
 * 
 *  @see com.splicemachine.db.iapi.db.Factory
 */
public interface Database
{

	/**
	 * Tells whether the Database is configured as read-only, or the
	 * Database was started in read-only mode.
	 *
	 * @return	TRUE means the Database is read-only, FALSE means it is
	 *		not read-only.
	 */
	public boolean		isReadOnly();

    /**
     * Backup the database to a backup directory.  See online documentation
     * for more detail about how to use this feature.
     *
     * @param backupDir the directory name where the database backup should
     *         go.  This directory will be created if not it does not exist.
     * @param wait if <tt>true</tt>, waits for  all the backup blocking 
     *             operations in progress to finish.
     * @exception SQLException Thrown on error
     */
    public void backup(String backupDir, boolean wait) 
        throws SQLException;

    public void restore(String restoreDir, boolean wait) 
            throws SQLException;

	/**
	  * Freeze the database temporarily so a backup can be taken.
	  * <P>Please see the Derby documentation on backup and restore.
	  *
	  * @exception SQLException Thrown on error
	  */
	public void freeze() throws SQLException;

	/**
	  * Unfreeze the database after a backup has been taken.
	  * <P>Please see the Derby documentation on backup and restore.
	  *
	  * @exception SQLException Thrown on error
	  */
	public void unfreeze() throws SQLException;

	/**
	 * Checkpoints the database, that is, flushes all dirty data to disk.
	 * Records a checkpoint in the transaction log, if there is a log.
	 *
	 * @exception SQLException Thrown on error
	 */
	public void checkpoint() throws SQLException;

	/**
	 * Get the Locale for this database.
	 */
	public Locale getLocale();

	/**
		Return the UUID of this database.
		@deprecated No longer supported.

	*/
	public UUID getId();
}	



