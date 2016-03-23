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

import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.gaoshin.dbshard2.entity.IndexedData;

public interface BaseDao extends Dao {
	ShardResolver getShardResolver();
	ShardedDataSource getShardedDataSource();
	int getDataSourceIdForObjectId(String id);
	
	<T> int create(T obj);
	<T> int update(T obj);
	<T> T getObject(Class<T> cls, String id);
	<T> List<T> objectLookup(Class<T> cls, List<String> ids);
	int delete(Class cls, String id);
	int deleteAll(Class cls, Collection<String> ids);
	
	List<IndexedData> indexLookup(final String sql, final Map<String, Object> values);
	int indexCountLookup(final String sql, final Map<String, Object> values);
	List<IndexedData> indexLookup(int dataSourceId, final String sql, final Map<String, Object> values);
	int indexCountLookup(int dataSourceId, final String sql, final Map<String, Object> values);
	int deleteIndexData(ClassIndex ind, String id) ;
	<T>List<T> query(int dataSourceId, String sql,
			Map<String, Object> params, RowMapper<T> rowMapper);
	<T>List<T> query(String sql,
			Map<String, Object> params, RowMapper<T> rowMapper);
	<T>List<T> query(String sql, RowMapper<T> rowMapper);
	
	int updateAll(String sql, Object...objects );
	int updateAll(String sql, Map<String, ?> params);

	<T> List<T> objectLookup(final Class<T> cls);
	<T> List<T> objectLookup(final Class<T> cls, int offset, int size);

	void dumpTable(OutputStream output, String tableName, String fields);
	
	NamedParameterJdbcTemplate getNamedParameterJdbcTemplate(int dataSourceId);
	JdbcTemplate getJdbcTemplate(int dataSourceId);
}
