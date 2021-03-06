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

package com.splicemachine.derby.impl.sql.catalog;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.sql.catalog.DD_Version;
import com.splicemachine.db.impl.sql.catalog.DataDictionaryImpl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jyuan on 10/13/14.
 */
public class Splice_DD_Version extends DD_Version {

    private int patchVersionNumber;

    public Splice_DD_Version() {

    }

    public Splice_DD_Version (DataDictionaryImpl bootingDictionary, int major, int minor, int patch) {
        this.bootingDictionary = bootingDictionary;
        this.majorVersionNumber = major;
        this.minorVersionNumber = minor;
        this.patchVersionNumber = patch;
    }

    public Splice_DD_Version (DataDictionaryImpl bootingDictionary, String versionString) throws StandardException {
    	if (versionString.matches("\\d(\\.\\d)?(\\.\\d)?")) {
            this.majorVersionNumber = 0;
            this.minorVersionNumber = 0;
            this.patchVersionNumber = 0;
    		String[] versionArray = versionString.split("\\.");
    		if (versionArray.length > 0) {
    			this.majorVersionNumber = Integer.parseInt(versionArray[0]);
    			if (versionArray.length > 1) {
    				this.minorVersionNumber = Integer.parseInt(versionArray[1]);
    				if (versionArray.length > 2) {
    					this.patchVersionNumber = Integer.parseInt(versionArray[2]);
    				}
    			}
    		}
    		this.bootingDictionary = bootingDictionary;
    	} else {
    		// Bad version string...
    		throw StandardException.newException(String.format("Version string does not match \"major.minor.patch\" pattern: %s", versionString));
    	}
    }

    public int getMajorVersionNumber() {
        return majorVersionNumber;
    }

    public int getMinorVersionNumber() {
        return minorVersionNumber;
    }

    public int getPatchVersionNumber() {
        return patchVersionNumber;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(majorVersionNumber).append(".").append(minorVersionNumber).append(".").append(patchVersionNumber);
        return sb.toString();
    }

    @Override
    public void readExternal( ObjectInput in ) throws IOException {
        super.readExternal(in);
        patchVersionNumber = in.readInt();
    }

    @Override
    public void writeExternal( ObjectOutput out ) throws IOException
    {
        super.writeExternal(out);
        out.writeInt(patchVersionNumber);
    }

    public long toLong() {
        return majorVersionNumber * 1000000 + minorVersionNumber * 1000 + patchVersionNumber;
    }

}
