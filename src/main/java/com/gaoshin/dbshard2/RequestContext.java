/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gaoshin.dbshard2;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

public class RequestContext {
	private static final Logger logger = Logger.getLogger(RequestContext.class);
	
    private static final ThreadLocal<RequestContext> localRequestContext = new ThreadLocal<>();
    
    public synchronized static RequestContext getRequestContext() {
        RequestContext context = localRequestContext.get();
    	logger.debug("RC context is " + context);
        if(context == null) {
            context = new RequestContext();
            localRequestContext.set(context);
        	logger.debug("RC create ");
        }
        context.id = UUID.randomUUID().toString();
        context.startTime = System.currentTimeMillis();
        return context;
    }
    
    public synchronized static void setRequestContext(RequestContext rc) {
        localRequestContext.set(rc);
    }
    
    public String id;
    public Long startTime;
    public Long endTime;
    
	private List<ExtendedDataSource> dataSourcesUsedByRequest = new ArrayList<ExtendedDataSource>();
	
	public void addDataSource(ExtendedDataSource dataSource) {
		synchronized (dataSourcesUsedByRequest) {
			dataSourcesUsedByRequest.add(dataSource);
		}
	}
	
	public void commit() throws SQLException {
    	logger.debug("RC commit");
		synchronized (dataSourcesUsedByRequest) {
			for(ExtendedDataSource ds : dataSourcesUsedByRequest) {
				ds.commitConnection(this);
			}
		}
	}
	
	public void rollback() throws SQLException {
    	logger.debug("RC rollback");
		synchronized (dataSourcesUsedByRequest) {
			for(ExtendedDataSource ds : dataSourcesUsedByRequest) {
				ds.rollbackConnection(this);
			}
		}
	}
	
	public void close() throws Exception {
    	logger.debug("RC close");
		synchronized (dataSourcesUsedByRequest) {
			for(ExtendedDataSource ds : dataSourcesUsedByRequest) {
				ds.closeConnection(this);
			}
			dataSourcesUsedByRequest.clear();
		}
	}
}
