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

package com.gaoshin.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;


public class ShardedDataSourceImpl implements ShardedDataSource {
	private static Logger logger = Logger.getLogger(ExtendedDataSource.class);

	private int shardsPerDataSource;
	private String userName;
	private String password;
	private String url;
	private String dbClassName;
	private int maxTotal = 10;
	private int maxIdle = 5;
	private boolean autoCommit = false;
	private String dbBaseName = null;
	
	private Map<Integer, ExtendedDataSource> dataSources;
	
	public ShardedDataSourceImpl() {
		setShardsPerDataSource(1);
		dataSources = new HashMap<Integer, ExtendedDataSource>();
	}

	@Override
	public ExtendedDataSource getDataSourceByShardId(RequestContext tc, int shardId) {
		int datasourceId = shardId / getShardsPerDataSource();
		return getDataSourceByDataSourceId(tc, datasourceId);
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getDbClassName() {
		return dbClassName;
	}

	public void setDbClassName(String dbClassName) {
		this.dbClassName = dbClassName;
	}

	@Override
	public Map<Integer, List<String>> splitByDataSource(Collection<String> ids) {
		Map<Integer, List<String>> map = new HashMap<Integer, List<String>>();
		for (String s : ids) {
			ObjectId oi = new ObjectId(s);
			int shard = oi.getShard();
			int datasource = shard / getShardsPerDataSource();
			List<String> list = map.get(datasource);
			if(list == null) {
				list = new ArrayList<String>();
				map.put(datasource, list);
			}
			list.add(s);
		}
		return map;
	}

	@Override
	public synchronized ExtendedDataSource getDataSourceByDataSourceId(RequestContext tc, int dataSourceId) {
		ExtendedDataSource ds = dataSources.get(dataSourceId);
		if(ds == null) {
			ds = new ExtendedDataSource();
			ds.setDriverClassName(dbClassName);
			ds.setUsername(userName);
			ds.setPassword(password);
			String dburl = (url != null? url.replaceAll("__DATASOURCEID__", String.valueOf(dataSourceId)):url);
			if(dbBaseName != null)
				dburl = dburl.replaceAll("DBBASENAME", dbBaseName);
			ds.setUrl(dburl);
			logger.debug("create data source for " + dburl);
			
			ds.setLogAbandoned(true);
			ds.setRemoveAbandonedTimeout(300);
			ds.setMinEvictableIdleTimeMillis(300000);
			ds.setTimeBetweenEvictionRunsMillis(30000);
	        ds.setMaxTotal(getMaxTotal());
	        ds.setMaxIdle(getMaxIdle());
	        ds.setMaxWaitMillis(2000);
	        ds.setMaxOpenPreparedStatements(500);
	        ds.setPoolPreparedStatements(false);
			ds.setAutoCommit(isAutoCommit());
			ds.setDataSourceId(dataSourceId);
			dataSources.put(dataSourceId, ds);
		}
		ds.setThreadContext(tc);
		return ds;
	}

	public int getShardsPerDataSource() {
		return shardsPerDataSource;
	}

	public void setShardsPerDataSource(int shardsPerDataSource) {
		this.shardsPerDataSource = shardsPerDataSource;
	}

	@Override
	public ExtendedDataSource getDataSourceByObjectId(RequestContext tc, String id) {
		ObjectId oi = new ObjectId(id);
		return getDataSourceByShardId(tc, oi.getShard());
	}

	public int getMaxTotal() {
		return maxTotal;
	}

	public void setMaxTotal(int maxTotal) {
		this.maxTotal = maxTotal;
	}

	public int getMaxIdle() {
		return maxIdle;
	}

	public void setMaxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
	}

	public boolean isAutoCommit() {
		return autoCommit;
	}

	public void setAutoCommit(boolean autoCommit) {
		this.autoCommit = autoCommit;
	}

	public String getDbBaseName() {
		return dbBaseName;
	}

	public void setDbBaseName(String dbBaseName) {
		this.dbBaseName = dbBaseName;
	}

}
