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

package com.gaoshin.dbshard2.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.gaoshin.dbshard2.BaseDao;
import com.gaoshin.dbshard2.ClassIndex;
import com.gaoshin.dbshard2.ExtendedDataSource;
import com.gaoshin.dbshard2.ObjectId;
import com.gaoshin.dbshard2.ShardResolver;
import com.gaoshin.dbshard2.ShardedDataSource;
import com.gaoshin.dbshard2.TableManager;
import com.gaoshin.dbshard2.entity.IndexedData;
import com.gaoshin.dbshard2.entity.ObjectData;
import common.util.JacksonUtil;
import common.util.MultiTask;
import common.util.reflection.ReflectionUtil;

@SuppressWarnings({"rawtypes","unchecked","static-access"}) 
public class BaseDaoImpl implements BaseDao {
	private static Logger logger = Logger.getLogger(BaseDaoImpl.class);
	
	protected ShardResolver shardResolver;
	protected ShardedDataSource shardedDataSource;
	protected ExecutorService executorService;
	protected TableManager tableManager;
	
	public ShardResolver getShardResolver() {
		return shardResolver;
	}
	public void setShardResolver(ShardResolver shardResolver) {
		this.shardResolver = shardResolver;
	}
	public ShardedDataSource getShardedDataSource() {
		return shardedDataSource;
	}
	public void setShardedDataSource(ShardedDataSource shardedDataSource) {
		this.shardedDataSource = shardedDataSource;
	}
	public ExecutorService getExecutorService() {
		return executorService;
	}
	public void setExecutorService(ExecutorService executorService) {
		this.executorService = executorService;
	}
	public TableManager getTableManager() {
		return tableManager;
	}
	public void setTableManager(TableManager tableManager) {
		this.tableManager = tableManager;
	}
	
	@Override
	public int getDataSourceIdForObjectId(String id) {
		return shardedDataSource.getDataSourceByObjectId(id).getDataSourceId();
	}
	
    public int getDataSourceIdForObject(ObjectData od) {
        if(od.id != null)
            return getDataSourceIdForObjectId(od.id);
        int shardId = shardResolver.getShardId(od);
        return shardedDataSource.getDataSourceByShardId(shardId).getDataSourceId();
    }

	@Override
	public int create(Class cls, final ObjectData obj) {
		if(!obj.getClass().equals(ObjectData.class))
			throw new RuntimeException("Please call createBean method");
		
		if(obj.json == null)
			throw new RuntimeException("json should not be null");
		final ObjectId oi = new ObjectId(obj.id);
		final AtomicInteger ups = new AtomicInteger();
		ExtendedDataSource dataSource = shardedDataSource.getDataSourceByShardId(oi.getShard());
		String sql = "insert into " + cls.getSimpleName() + " (id, created, json) values (?, ?, ?)";
		logger.debug(sql + "\n" + obj.json);
		JdbcTemplate jt = dataSource.getJdbcTemplate();
		int res = jt.update(sql, obj.id, obj.created, obj.json);
		ups.getAndAdd(res);
		return ups.get();
	}
	
	@Override
	public int update(Class cls, final ObjectData obj) {
		if(obj.json == null)
			throw new RuntimeException("json should not be null");
		final ObjectId oi = new ObjectId(obj.id);
		final AtomicInteger ups = new AtomicInteger();
		ExtendedDataSource dataSource = shardedDataSource.getDataSourceByShardId(oi.getShard());
		String sql = "update " + cls.getSimpleName() + " set created=?, json=? where id=?";
		JdbcTemplate jt = dataSource.getJdbcTemplate();
		int res = jt.update(sql, obj.created, obj.json, obj.id);
		logger.debug("update " + obj.id + " with json " + obj.json);
		ups.getAndAdd(res);
		return ups.get();
	}

	@Override
	public ObjectData objectLookup(Class cls, String id) {
		ObjectData data = null;
		if(id != null){
				ObjectId oi = new ObjectId(id);
				ExtendedDataSource dataSource = shardedDataSource.getDataSourceByShardId(oi.getShard());
				String sql = "select * from " + cls.getSimpleName() + " where id=?";
				JdbcTemplate jt = dataSource.getJdbcTemplate();
				List<ObjectData> od = jt.query(sql, new Object[]{id}, new ObjectDataRowMapper());
				
				data = od.size() > 0 ? od.get(0) : null;
		}
		return data;
	}
	
	@Override
	public List<ObjectData> sqlLookup(final String sql) {
		final List<ObjectData> result = new ArrayList<>();
		forEachDataSource(new ShardRunnable() {
					@Override
					public void run(int dataSourceId) {
						ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(dataSourceId);
						NamedParameterJdbcTemplate namedjc = dataSource.getNamedParameterJdbcTemplate();
						List<ObjectData> data = namedjc.query(sql, new ObjectDataRowMapper());
						synchronized (result) {
							result.addAll(data);
						}
					}
				});
		return result;
	}

	@Override
	public List<ObjectData> sqlLookup(final String sql, int dataSourceId) {
		ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(dataSourceId);
		NamedParameterJdbcTemplate namedjc = dataSource.getNamedParameterJdbcTemplate();
		List<ObjectData> data = namedjc.query(sql, new ObjectDataRowMapper());
		return data;
	}

	@Override
	public NamedParameterJdbcTemplate getNamedParameterJdbcTemplate(int dataSourceId) {
		ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(dataSourceId);
		NamedParameterJdbcTemplate namedjc = dataSource.getNamedParameterJdbcTemplate();
		return namedjc;
	}

	@Override
	public JdbcTemplate getJdbcTemplate(int dataSourceId) {
		ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(dataSourceId);
		JdbcTemplate namedjc = dataSource.getJdbcTemplate();
		return namedjc;
	}

	@Override
	public List<ObjectData> objectLookup(Class cls, Collection<String> ids) {
		HashMap<String, ObjectData> map = new HashMap<String, ObjectData>();
		List<ObjectData> result = new ArrayList<>();
		if(ids == null || ids.size()==0){
			return result;
		}
		List<String> spilledIdList = new ArrayList<>(ids);
		if(spilledIdList.size() > 0){
			ObjectId oi = new ObjectId(spilledIdList.get(0));
			final String sql = "select * from " + cls.getSimpleName() + " where id in (:ids)";
			final Map<Integer, List<String>> shardedIds = shardedDataSource.splitByDataSource(spilledIdList);
			final List<ObjectData> spilledResults = new ArrayList<>();
			
			forSelectDataSources(shardedIds.keySet(),new ShardRunnable() {
				@Override
				public void run(int dataSourceId) {
					ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(dataSourceId);
					NamedParameterJdbcTemplate namedjc = dataSource.getNamedParameterJdbcTemplate();
					List<ObjectData> data = namedjc.query(sql, Collections.singletonMap("ids", shardedIds.get(dataSourceId)), new ObjectDataRowMapper());
					synchronized (spilledResults) {
						spilledResults.addAll(data);
					}
				}
			});

			HashMap<String,Object> spilledMap = new HashMap<>();
			for (ObjectData objectData : spilledResults) {
				spilledMap.put(objectData.id, objectData);
				spilledIdList.remove(objectData.id);
				map.put(objectData.id, objectData);
			}

		}
		
		for(String id : ids) {
			ObjectData data = map.get(id);
			if(data != null)
				result.add(data);
		}
		return result;
	}

	@Override
	public int delete(Class cls, String id) {
		ObjectId oi = new ObjectId(id);
		ExtendedDataSource dataSource = shardedDataSource.getDataSourceByShardId(oi.getShard());
		String sql = "delete from " + cls.getSimpleName() + " where id=?";
		JdbcTemplate jt = dataSource.getJdbcTemplate();
		int res = jt.update(sql, id);
		return res;
	}

	@Override
	public int deleteAll(Class cls, final Collection<String> ids) {
		final AtomicInteger ups = new AtomicInteger();
		if(ids.size() > 0){
			ObjectId oi = new ObjectId(ids.iterator().next());
			final String sql = "delete from " + cls.getSimpleName() + " where id in (:ids)";
			final Map<Integer, List<String>> shardedIds = shardedDataSource.splitByDataSource(ids);
			
			forSelectDataSources(shardedIds.keySet(), new ShardRunnable() {
				@Override
				public void run(int shardId) {
					ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(shardId);
					NamedParameterJdbcTemplate namedjc = dataSource.getNamedParameterJdbcTemplate();
					int update = namedjc.update(sql, Collections.singletonMap("ids", shardedIds.get(shardId)));
					synchronized (ups) {
						ups.getAndAdd(update);
					}
				}
			});
			
		}
		return ups.intValue();
	}
	
	protected int createIndex(ClassIndex index, Map<String, Object>values) {
		String table = index.getTableName();
		StringBuilder sql = new StringBuilder("insert into ").append(table).append("(");
		StringBuilder valueNames = new StringBuilder();
		valueNames.append(" values (");
		boolean first = true;
		for(String s : values.keySet().toArray(new String[0])) {
			if(first) {
				first = false;
			}else {
				sql.append(",");
				valueNames.append(",");
			}
			sql.append(s);
			valueNames.append(":").append(s);
			Object value = values.get(s);
			if(value != null && value instanceof Enum<?>) {
				values.put(s, value.toString());
			}
		}
		sql.append(")");
		valueNames.append(")");
		sql.append(valueNames.toString());
		
		ObjectId objectId = new ObjectId((String)values.get("id"));
		ExtendedDataSource dataSource = shardedDataSource.getDataSourceByShardId(objectId.getShard());
		NamedParameterJdbcTemplate namedjc = dataSource.getNamedParameterJdbcTemplate();
		int res = namedjc.update(sql.toString(), values);
		return res;
	}

	@Override
	public List<IndexedData> indexLookup(final String sql, final Map<String, Object> values) {
		final List<IndexedData> result = new ArrayList<>();
		ShardRunnable runnable = new ShardRunnable() {
			@Override
			public void run(int dataSourceId) {
				ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(dataSourceId);
				NamedParameterJdbcTemplate namedjc = dataSource.getNamedParameterJdbcTemplate();
				List<IndexedData> data = namedjc.query(sql.toString(), values, new IndexedDataRowMapper());
				synchronized (result) {
					result.addAll(data);
				}
			}
		};
		forEachDataSource(runnable);
		return result;
	}

	@Override
	public List<IndexedData> indexLookup(int dataSourceId, final String sql, final Map<String, Object> values) {
		ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(dataSourceId);
		NamedParameterJdbcTemplate namedjc = dataSource.getNamedParameterJdbcTemplate();
		List<IndexedData> data = namedjc.query(sql.toString(), values, new IndexedDataRowMapper());
		return data;
	}

	@Override
	public int indexCountLookup(final String sql, final Map<String, Object> values) {
		final AtomicInteger ai = new AtomicInteger();
		ShardRunnable runnable = new ShardRunnable() {
			@Override
			public void run(int dataSourceId) {
				ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(dataSourceId);
				NamedParameterJdbcTemplate namedjc = dataSource.getNamedParameterJdbcTemplate();
				int count = namedjc.queryForObject(sql.toString(), values, Integer.class);
				ai.addAndGet(count);
			}
		};
		forEachDataSource(runnable);
		return ai.intValue();
	}

	@Override
	public int indexCountLookup(int dataSourceId, final String sql, final Map<String, Object> values) {
		ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(dataSourceId);
		NamedParameterJdbcTemplate namedjc = dataSource.getNamedParameterJdbcTemplate();
		int count = namedjc.queryForObject(sql.toString(), values, Integer.class);
		return count;
	}
	
	protected int indexedCountLookup(final ClassIndex index, final Map<String, Object> values) {
		String table = index.getTableName();
		final StringBuilder sql = new StringBuilder("select count(*) from ").append(table);
		boolean first = true;
		Boolean emptyCollection = null;
		Map<String, Object> params = new HashMap<String, Object>();
		for(String s : values.keySet()) {
			String columnName = s.toString().replaceAll("\\.", "__");
			if(first) {
				sql.append(" where ");
				first = false;
			}else {
				sql.append(" and ");
			}
			Object value = values.get(s);
			params.put(columnName, value);
			if(value == null || value instanceof Collection) {
				sql.append(columnName).append(" in (:").append(columnName).append(")");
				Collection c = (Collection) value;
				emptyCollection = c.size() == 0;
			}
			else {
				sql.append(columnName).append("=:").append(columnName);
			}
		}
		
		if(Boolean.TRUE.equals(emptyCollection)) 
			return 0;
		else
			return indexCountLookup(sql.toString(), params);
	}
	
	protected int indexedCountLookup(final ClassIndex index, final Map<String, Object> values, int dataSourceId) {
		String table = index.getTableName();
		final StringBuilder sql = new StringBuilder("select count(*) from ").append(table);
		boolean first = true;
		Boolean emptyCollection = null;
		Map<String, Object> params = new HashMap<String, Object>();
		for(String s : values.keySet()) {
			String columnName = s.toString().replaceAll("\\.", "__");
			if(first) {
				sql.append(" where ");
				first = false;
			}else {
				sql.append(" and ");
			}
			Object value = values.get(s);
			params.put(columnName, value);
			if(value == null || value instanceof Collection) {
				sql.append(columnName).append(" in (:").append(columnName).append(")");
				Collection c = (Collection) value;
				emptyCollection = c.size() == 0;
			}
			else {
				sql.append(columnName).append("=:").append(columnName);
			}
		}
		
		if(Boolean.TRUE.equals(emptyCollection))
			return 0;
		else
			return indexCountLookup(dataSourceId, sql.toString(), params);
	}
	
	protected List<IndexedData> indexedLookup(Integer dataSourceId, final ClassIndex index, final Map<String, Object> values) {
		String key = getTableManager().getIndexedLookupKey(index, values);
		Map<String, Object> params = new HashMap<String, Object>();
		String table = index.getTableName();
		final StringBuilder sql = new StringBuilder("select * from ").append(table);
		boolean first = true;
		Boolean emptyCollection = null;
		for(Object s : values.keySet().toArray()) {
			String columnName = s.toString().replaceAll("\\.", "__");
			if(first) {
				sql.append(" where ");
				first = false;
			}else {
				sql.append(" and ");
			}
			Object value = values.get(s);
			params.put(columnName, value);
			if(value == null || value instanceof Collection) {
				sql.append(columnName).append(" in (:").append(columnName).append(")");
				Collection c = (Collection) value;
				emptyCollection = ((c == null) || (c.size() == 0));
			}
			else {
				sql.append(columnName).append("=:").append(columnName);
			}
		}
		sql.append(" order by created desc ");
		
		List<IndexedData> result = null;
		if(Boolean.TRUE.equals(emptyCollection)) 
			result = new ArrayList<IndexedData>();
		else if(dataSourceId != null)
			result = indexLookup(dataSourceId, sql.toString(), params);
		else 
			result = indexLookup(sql.toString(), params);
		return result;
	}

	@Override
	public int deleteIndexData(ClassIndex ind, final String id) {
        final String sql = "delete from " + ind.getTableName() + " where id = :id";
        ExtendedDataSource dataSource = shardedDataSource.getDataSourceByObjectId(id);
        NamedParameterJdbcTemplate namedjc = dataSource.getNamedParameterJdbcTemplate();
        int update = namedjc.update(sql, Collections.singletonMap("id", id));
        return update;
	}

	@Override
	public int updateAll(final String sql, final Object... objects) {
	    System.out.println("Update all: " + sql);
		final AtomicInteger ups = new AtomicInteger();
		forEachDataSource(new ShardRunnable() {
			@Override
			public void run(int dataSourceId) {
				ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(dataSourceId);
				JdbcTemplate jc = dataSource.getJdbcTemplate();
				int update = jc.update(sql, objects);
				logger.debug(dataSourceId + " DBUpdate: " + sql);
				synchronized (ups) {
					ups.getAndAdd(update);
				}
			}
		});
		return ups.intValue();
	}

	@Override
	public int updateAll(final String sql, final Map<String, ?> params) {
		final AtomicInteger ups = new AtomicInteger();
		forEachDataSource(new ShardRunnable() {
			@Override
			public void run(int dataSourceId) {
				ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(dataSourceId);
				NamedParameterJdbcTemplate jc = dataSource.getNamedParameterJdbcTemplate();
				int update = jc.update(sql, params);
				synchronized (ups) {
					ups.getAndAdd(update);
				}
			}
		});
		return ups.intValue();
	}
	
	public void forEachDataSource(ShardRunnable runnable){
		MultiTask mt = new MultiTask();
		for(int i= 0; i<shardResolver.getNumberOfShards() / shardedDataSource.getShardsPerDataSource(); i++) {
			ShardTask shardTask = new ShardTask(i, runnable);
			mt.addTask(shardTask);
		}
		mt.execute(getExecutorService());
	}
	
	public void forEachDataSourceOneByOne(ShardRunnable runnable){
		for(int i= 0; i<shardResolver.getNumberOfShards() / shardedDataSource.getShardsPerDataSource(); i++) {
			runnable.run(i);
		}
	}
	
	public void forSelectDataSources(Collection<Integer> dataSourceIds, ShardRunnable runnable){
	    if(dataSourceIds.size()>1) {
    		MultiTask mt = new MultiTask();
    		for(Integer dataSourceId : dataSourceIds) {
    			ShardTask shardTask = new ShardTask(dataSourceId, runnable);
    			mt.addTask(shardTask);
    		}
    		mt.execute(getExecutorService());
	    }
	    else if(dataSourceIds.size() == 1) {
	        int dataSourceId = (int) dataSourceIds.toArray()[0];
            ShardTask shardTask = new ShardTask(dataSourceId, runnable);
            shardTask.run();
	    }
	}
	
	static class ObjectDataRowMapper<T extends ObjectData> implements RowMapper<T>{
		private Class cls = null;
		public ObjectDataRowMapper() {
			try {
				if(this.getClass().equals(ObjectDataRowMapper.class))
					cls = ObjectData.class;
				else
					cls = ReflectionUtil.getParameterizedType(this.getClass());
			} catch (Exception e) {
			}
		}
		
		public ObjectDataRowMapper(Class cls) {
			this.cls = cls;
		}
		
		@Override
		public T mapRow(ResultSet arg0, int arg1)
				throws SQLException {
			try {
				T row = (T) cls.newInstance();
				row.id = arg0.getString("id");
				row.created = arg0.getLong("created");
				row.json = arg0.getString("json");
				return row;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	@Override
	public <T extends ObjectData> List<T> objectLookup(final Class<T> cls) {
		return objectLookup(cls, -1, -1);
	}
	
	@Override
	public <T extends ObjectData> List<T> objectLookup(final Class<T> cls, int offset, int size) {
		final Map<String, Object> keyValues = new HashMap<String, Object>();
		final StringBuilder sb = new StringBuilder();
		sb.append("select * from ").append(cls.getSimpleName());
		boolean first = true;
		for(Entry<String, Object>entry : keyValues.entrySet()) {
			if(first) {
				sb.append(" where ");
				first = false;
			}else {
				sb.append(" and ");
			}
			sb.append(entry.getKey()).append(" = :").append(entry.getKey());
		}
		sb.append(" order by created desc ");
		
		if(offset>-1) {
			sb.append(" limit :offset, :size");
			keyValues.put("offset", offset);
			keyValues.put("size", size);
		}
		
		final List<T> results = new ArrayList<T>();
		forEachDataSource(new ShardRunnable() {
			@Override
			public void run(int dataSourceId) {
				ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(dataSourceId);
				NamedParameterJdbcTemplate jc = dataSource.getNamedParameterJdbcTemplate();
				List<T> dsResults = jc.query(sb.toString(), keyValues, new ObjectDataRowMapper(cls));
				synchronized (results) {
					results.addAll(dsResults);
				}
			}
		});
		return results;
	}
	
	@Override
	public void dumpTable(final OutputStream output, final String tableName, final String fields) {
		final String[] names = fields == null ? null : fields.split(",");;
		forEachDataSourceOneByOne(new ShardRunnable() {
			@Override
			public void run(int dataSourceId) {
				String sql = "select json from " + tableName.replaceAll(" ", "");
				ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(dataSourceId);
				JdbcTemplate jc = dataSource.getJdbcTemplate();
				jc.query(sql, new RowCallbackHandler(){
					@Override
					public void processRow(ResultSet rs) throws SQLException {
						try {
							String json = rs.getString(1);
							if(names == null)
								output.write(json.getBytes());
							else {
								HashMap<String, Object> map = JacksonUtil.json2map(json);
								boolean first = true;
								for(String name : names) {
									if(first) {
										first = false;
									}
									else {
										output.write('\t');
									}
									Object obj = map.get(name);
									if(obj == null)
										output.write("NULL".getBytes());
									else
										output.write(obj.toString().getBytes());
								}
							}
							output.write('\n');
						} catch (IOException e) {
							throw new SQLException(e);
						}
					}});
			}
		});
	}
	
	@Override
	public <T> List<T> indexQuery(int dataSourceId, String sql,
			Map<String, Object> params, RowMapper<T> rowMapper) {
		ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(dataSourceId);
		NamedParameterJdbcTemplate namedjc = dataSource.getNamedParameterJdbcTemplate();
		return namedjc.query(sql, params, rowMapper);
	}
	
	@Override
	public <T> List<T> indexQuery(final String sql, final Map<String, Object> params,
			final RowMapper<T> rowMapper) {
		final List<T> result = new ArrayList<T>();
		forEachDataSource(new ShardRunnable() {
			@Override
			public void run(int dataSourceId) {
				ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(dataSourceId);
				NamedParameterJdbcTemplate namedjc = dataSource.getNamedParameterJdbcTemplate();
				List<T> list = namedjc.query(sql, params, rowMapper);
				synchronized (result) {
					result.addAll(list);
				}
			}
		});
		return result;
	}
	
	@Override
	public <T> List<T> indexQuery(final String sql, final RowMapper<T> rowMapper) {
		final List<T> result = new ArrayList<T>();
		forEachDataSource(new ShardRunnable() {
			@Override
			public void run(int dataSourceId) {
				ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(dataSourceId);
				NamedParameterJdbcTemplate namedjc = dataSource.getNamedParameterJdbcTemplate();
				List<T> list = namedjc.query(sql, rowMapper);
				synchronized (result) {
					result.addAll(list);
				}
			}
		});
		return result;
	}

}

