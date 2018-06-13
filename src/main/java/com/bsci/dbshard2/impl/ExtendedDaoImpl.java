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

package com.bsci.dbshard2.impl;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import com.bsci.dbshard2.entity.IndexedData;
import com.bsci.dbshard2.entity.MappedData;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.bsci.dbshard2.BeanManager;
import com.bsci.dbshard2.ClassIndex;
import com.bsci.dbshard2.ClassMapping;
import com.bsci.dbshard2.ClassSqls;
import com.bsci.dbshard2.ClassTable;
import com.bsci.dbshard2.ColumnPath;
import com.bsci.dbshard2.ColumnValues;
import com.bsci.dbshard2.DaoManager;
import com.bsci.dbshard2.DbDialet;
import com.bsci.dbshard2.DbShardUtils;
import com.bsci.dbshard2.ExtendedDao;
import com.bsci.dbshard2.ExtendedDataSource;
import com.bsci.dbshard2.ObjectId;
import com.bsci.dbshard2.ObjectOperation;
import com.bsci.dbshard2.TimeRange;
import common.util.DateUtil;
import common.util.reflection.ReflectionUtil;

public class ExtendedDaoImpl extends BaseDaoImpl implements ExtendedDao {
	private static Logger logger = Logger.getLogger(ExtendedDaoImpl.class);
	
	private List<Class> forClasses;
	
	public ExtendedDaoImpl(Class... forClass){
		forClasses = new ArrayList<Class>();
		for(Class cls : forClass)
			addClass(cls);
	}
	
	public void addClass(Class forcls) {
		if(forClasses.contains(forcls))
			return;
		forClasses.add(forcls);
		DaoManager.getInstance().add(forcls, this);
	}
	
	@Override
	public String generateIdForBean(Object obj) {
		int shardId = getShardResolver().getShardId(obj);
		ObjectId oi = new ObjectId(shardId);
		String id = oi.toString();
		return id;
	}	
	
	@Override
	public String generateSameShardId(String id) {
		ObjectId oi = new ObjectId(id);
		oi.setUuid(UUID.randomUUID().toString());
		return oi.toString();
	}
	
	@Override
	public void createBean(Object obj) {
		if(!forClasses.contains(obj.getClass()))
			throw new RuntimeException("cannot use " + this.getClass().getSimpleName() + " to create " + obj.getClass());

		String id = getId(obj);
		if(id == null) {
			id = generateIdForBean(obj);
			ReflectionUtil.setFieldValue(obj, "id", id);
		}
		
		Long created = getCreated(obj);
		if(created == null || created == 0) {
		    created = DateUtil.currentTimeMillis();
            ReflectionUtil.setFieldValue(obj, "created", created);
		}
		
		super.create(obj);
		addIndexesForBean(obj);
		addMappingsForBean(obj);
	}
	
	@Override
	public void touchAllBean(Class cls) {
		forEachBean(cls, new BeanHandler() {
			@Override
			public void processBean(Object bean) {
				updateBeanAndIndexAndMapping(bean);
			}
		});
	}

    @Override
    public void updateBeanAndIndexAndMapping(Object obj) {
        super.update(obj);
        removeIndexesForBean(obj);
        addIndexesForBean(obj);
        removeMappingsForBean(obj);
        addMappingsForBean(obj);
    }
	
	protected int addIndexesForBean(Object obj){
		ClassIndex[] indexes = getTableForBean(obj).getIndexes();
		if(indexes == null)
			return 0;
		int ret = 0;
		for(ClassIndex index : indexes) {
			ret += addIndexForBean(index, obj);
		}
		return ret;
	}
	
	protected int addMappingsForBean(Object obj){
		int ret = 0;
		ClassMapping[] mappings = getTableForBean(obj).getMappings();
		if(mappings == null) 
			return 0;
		
		for(ClassMapping mapping : mappings) {
			Class primaryCls = mapping.map2cls;
			ExtendedDao dao = getDaoForClass(primaryCls);
			
			String table = mapping.getTableName();
			StringBuilder sql = new StringBuilder().append("insert into ").append(table).append(" (pid, sid, created");
			if(mapping.otherColumns != null) {
				for(String s : mapping.otherColumns) {
					String columnName = new ColumnPath(s).getColumnName();
					sql.append(",").append(columnName);
				}
			}
			sql.append(")").append(" values (?, ?, ?");
			if(mapping.otherColumns != null) {
				for(String s : mapping.otherColumns) {
					sql.append(", ?");
				}
			}
			sql.append(")");

			int size = mapping.otherColumns == null ? 3 : mapping.otherColumns.length + 3;
			Object[] values = new Object[size];
			try {
				values[0] = ReflectionUtil.getFieldValue(obj, mapping.column);
				if(values[0] == null)
					continue;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			values[1] = getId(obj);
			values[2] = getCreated(obj);
			
			
			if(mapping.otherColumns != null) {
				for(int i = 0; i< mapping.otherColumns.length; i++) {
					try {
						values[i+3] = ReflectionUtil.getFieldValue(obj, mapping.otherColumns[i]);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			}

			ExtendedDataSource dataSource = dao.getShardedDataSource().getDataSourceByObjectId((String)values[0]);
			JdbcTemplate jt = dataSource.getJdbcTemplate();
			ret += jt.update(sql.toString(), values);
		}
		return ret;
	}

	public static ExtendedDao getDaoForClass(Class cls) {
		return DaoManager.getInstance().get(cls);
	}
	
	protected int addIndexForBean(ClassIndex ti, Object obj){
		List<ColumnValues> columnValues = new ArrayList<ColumnValues>();
		for(String indexColumnName : ti.columns) {
			try {
				List<Object> list = new ArrayList();
				getValues(obj, indexColumnName, list);
				String columnName = new ColumnPath(indexColumnName).getColumnName();
				columnValues.add(new ColumnValues(columnName, list));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		
		int ret = 0;
		List<Map> values = valueCombination(columnValues);
		for(Map m : values) {
			m.put("id", getId(obj));
			m.put("created", getCreated(obj));
			ret += super.createIndex(ti, m);
		}
		
		return ret;
	}
	
	public static List<Map> valueCombination(List<ColumnValues> columnValues) {
		if(columnValues.size()==1) {
			List<Map> list = new ArrayList<Map>();
			ColumnValues cv = columnValues.get(0);
			for(Object v : cv.values) {
				Map m = new HashMap();
				m.put(cv.name, v);
				list.add(m);
			}
			return list;
		}
		
		List<Map> ret = new ArrayList<Map>();
		ColumnValues cv = columnValues.get(0);
		for(Object v : cv.values) {
			List<Map> list = valueCombination(columnValues.subList(1, columnValues.size()));
			for(Map m : list) {
				m.put(cv.name, v);
			}
			ret.addAll(list);
		}
		return ret;
	}
	
	private static void getValues(Object obj, String indexColumnName, List<Object>values) {
		try {
			int pos = indexColumnName.indexOf('.');
			if(pos == -1) {
				Object value = ReflectionUtil.getFieldValue(obj, indexColumnName);
				if(value != null) {
					if(value instanceof List) {
						for(Object o : (List)value) {
							values.add(o);
						}
					}
					else {
						values.add(value);
					}
				}
				else {
					values.add(value);
				}
				return;
			}
			
			String fieldName = indexColumnName.substring(0, pos);
			Field field = ReflectionUtil.getField(obj.getClass(), fieldName);
			Object value = ReflectionUtil.getFieldValue(obj, fieldName);
			if(List.class.isAssignableFrom(field.getType())) {
				if(value != null) {
					List list = (List)value;
					for(Object item : list) {
						if(item != null) {
							getValues(item, indexColumnName.substring(pos+1), values);
						}
					}
				}
			}
			else {
				if(value != null) {
					getValues(value, indexColumnName.substring(pos+1), values);
				}
			}
		} catch (Exception e) {
		}
	}
	
	public ClassTable getTableForBean(Object obj) {
		return getTableManager().getTable(obj.getClass());
	}
	
	protected int removeIndexesForBean(Object obj){
		int ret = 0;
		ClassIndex[] indexes = getTableForBean(obj).getIndexes();
		if(indexes == null)
			return ret;
		for(ClassIndex index : indexes) {
			deleteIndexData(index, (String)ReflectionUtil.getFieldValue(obj, "id"));
		}
		return ret;
	}
	
	@Override
	public int delete(Class cls, String id) {
		Object od = getObject(cls, id);
		removeIndexesForBean(od);
		removeMappingsForBean(od);
		return super.delete(cls, id);
	}
	
	@Override
	public int deleteBean(Object od) {
		removeIndexesForBean(od);
		removeMappingsForBean(od);
		return super.delete(od.getClass(), getId(od));
	}
	
	@Override
	public int deleteAll(Class cls, Collection<String> ids) {
		int total = 0;
		for(String id : ids) {
			total += delete(cls, id);
		}
		return total;
	}
	
	protected int removeMappingsForBean(Object obj){
		int ret = 0;
		ClassMapping[] mappings = getTableForBean(obj).getMappings();
		if(mappings == null)
			return 0;
		for(ClassMapping mapping : mappings) {
			String table = mapping.getTableName();
			List<Object> list = new ArrayList();
			getValues(obj, mapping.column, list);
			for(Object sid : list) {
				ExtendedDao dao = getDaoForClass(mapping.map2cls);
				ExtendedDataSource dataSource = dao.getShardedDataSource().getDataSourceByObjectId((String)sid);
				JdbcTemplate jt = dataSource.getJdbcTemplate();
				String sql = "delete from " + table + " where sid='" + ReflectionUtil.getFieldValue(obj, "id") + "'";
				ret += jt.update(sql);
			}
		}
		return ret;
	}
	
	@Override
	public void removeBeans(List list) {
		for(Object od : list) {
		    deleteBean(od);
		}
	}

	@Override
	public <T> List<T> indexLookup(Class<T> cls, String field, Object value, int dataSourceId){
		ClassIndex index = indexByKeys(cls, Arrays.asList(field));
		List<String> ids = new ArrayList<>();
		for(IndexedData id: super.indexedLookup(dataSourceId, index,Collections.singletonMap(field, value))){
			ids.add(id.getId());
		}
		return objectLookup(cls, ids);
	}

	@Override
	public <T> List<T> indexLookup(Class<T> cls, String field, Object value){
		ClassIndex index = indexByKeys(cls, Arrays.asList(field));
		List<String> ids = new ArrayList<>();
		for(IndexedData id: super.indexedLookup(null, index,Collections.singletonMap(field, value))){
			ids.add(id.getId());
		}
		return objectLookup(cls, ids);
	}

	@Override
	public int indexCountLookup(Class cls, String field, Object value, int dataSourceId){
		ClassIndex index = indexByKeys(cls, Arrays.asList(field));
		return super.indexedCountLookup(index,Collections.singletonMap(field, value), dataSourceId);
	}

	@Override
	public <T> List<T> indexLookup(Class<T> cls,
			Map<String, Object> keyValues) {
		ClassIndex index = indexByKeys(cls, keyValues.keySet());
		List<String> ids = new ArrayList<>();
		if(index != null){
			for(IndexedData id: super.indexedLookup(null, index,keyValues)){
				ids.add(id.getId());
			}
		}
		return objectLookup(cls, ids);
	}

	@Override
	public <T> List<T> indexLookup(Class<T> cls,
			Map<String, Object> keyValues, int dataSourceId) {
		ClassIndex index = indexByKeys(cls, keyValues.keySet());
		List<String> ids = new ArrayList<>();
		if(index != null){
			for(IndexedData id: super.indexedLookup(dataSourceId, index,keyValues)){
				ids.add(id.getId());
			}
		}
		return objectLookup(cls, ids);
	}

	@Override
	public int indexCountLookup(Class cls,
			Map<String, Object> keyValues, int dataSourceId) {
		ClassIndex index = indexByKeys(cls, keyValues.keySet());
		return super.indexedCountLookup(index,keyValues, dataSourceId);
	}
	
	@Override
	public int indexCountLookup(Class cls, String field, Object value) {
		return indexCountLookup(cls, Collections.singletonMap(field, value));
	}
	
	@Override
	public int indexCountLookup(Class cls, Map<String, Object> keyValues) {
		ClassIndex index = indexByKeys(cls, keyValues.keySet());
		return indexedCountLookup(index, keyValues);
	}

	@Override
	public <T> List<T> indexLookup(Class<T> cls, String field, String indexedId, int offset, int size){
		ClassIndex index = indexByKeys(cls, Arrays.asList(field));
		ExtendedDataSource dataSource = shardedDataSource.getDataSourceByObjectId(indexedId);
		NamedParameterJdbcTemplate namedjc = dataSource.getNamedParameterJdbcTemplate();
		String sql = "select * from " + index.getTableName() + " where " + field + " = :field limit " + offset + ", " + size;
		List<IndexedData> data = namedjc.query(sql.toString(), Collections.singletonMap("field", indexedId), new IndexedDataRowMapper());
		List<String> ids = new ArrayList<>();
		for(IndexedData id: data){
			ids.add(id.getId());
		}
		return objectLookup(cls, ids);
	}
	
	@Override
	public ClassIndex indexByKeys(Class forClass, String... keys) {
		List<String> list = new ArrayList<String>();
		for(String s : keys) {
			list.add(s);
		}
		return indexByKeys(forClass, list);
	}
	
	@Override
	public ClassIndex indexByKeys(Class forClass, Collection<String>keys) {
		ClassIndex ti = null;
		ClassTable table = getTableManager().getTable(forClass);
		int maxMatched = 0;
		for(ClassIndex ci : table.getIndexes()) {
			int matched = 0;
			for(String s : ci.columns) {
				if(keys.contains(s)) {
					matched++;
				}
			}
			if(matched>maxMatched) {
				ti = ci;
				maxMatched = matched;
			}
		}
		if(maxMatched == 0)
			throw new RuntimeException("index doesn't exist for " + keys);
		return ti;
	}

	@Override
	public Map<Class, ClassSqls> getCreateTableSqls(DbDialet dialet) {
		Map<Class, ClassSqls> result = new HashMap<>();
		for(Class cls : forClasses) {
		    ClassTable classTable = getTableManager().getTable(cls);
		    if(classTable == null) continue;
            Map<Class, ClassSqls> map = DbShardUtils.getSqls(classTable, dialet);
			for(Entry<Class, ClassSqls> entry : map.entrySet()) {
				Class key = entry.getKey();
				ClassSqls classSqls = result.get(key);
				if(classSqls == null) {
					result.put(key, entry.getValue());
				}
				else 
					classSqls.sqls.addAll(entry.getValue().sqls);
			}
		}
		return result;
	}
	
	@Override
	public void updateSqls(Map<Class, ClassSqls> sqls) {
		for(Entry<Class, ClassSqls> entry : sqls.entrySet()) {
			Class key = entry.getKey();
			if(forClasses.contains(key)) {
				for(String sql : entry.getValue().sqls) {
					logger.debug(sql);
					updateAll(sql);
				}
			}
		}
	}
	
	@Override
	public List<MappedData> mappedLookup(Class pclass, Class sclass, String pid) {
		ClassTable ct = getTableManager().getTable(sclass);
		ClassMapping cm = ct.getClassMapping(pclass);
		final ExtendedDataSource dataSource = getShardedDataSource().getDataSourceByObjectId(pid);
		final String sql = "select * from " + cm.getTableName() + " where pid = :pid";
		NamedParameterJdbcTemplate namedjc = dataSource.getNamedParameterJdbcTemplate();
		List<MappedData> data = namedjc.query(sql, Collections.singletonMap("pid", pid), new MappedDataRowMapper());
		return data;
	}
	
	@Override
	public int mappedCountLookup(Class pclass, Class sclass, String pid) {
		ClassTable ct = getTableManager().getTable(sclass);
		ClassMapping cm = ct.getClassMapping(pclass);
		final ExtendedDataSource dataSource = getShardedDataSource().getDataSourceByObjectId(pid);
		final String sql = "select count(*) from " + cm.getTableName() + " where pid = :pid";
		NamedParameterJdbcTemplate namedjc = dataSource.getNamedParameterJdbcTemplate();
		int cnt = namedjc.queryForObject(sql, Collections.singletonMap("pid", pid), Integer.class);
		return cnt;
	}
	
	@Override
	public List<MappedData> mappedLookup(Class pclass, Class sclass, String pid, int offset, int size) {
		ClassTable ct = getTableManager().getTable(sclass);
		ClassMapping cm = ct.getClassMapping(pclass);
		final ExtendedDataSource dataSource = getShardedDataSource().getDataSourceByObjectId(pid);
		final String sql = "select * from " + cm.getTableName() + " where pid = :pid order by created desc limit " + offset + "," + size;
		NamedParameterJdbcTemplate namedjc = dataSource.getNamedParameterJdbcTemplate();
		List<MappedData> data = namedjc.query(sql, Collections.singletonMap("pid", pid), new MappedDataRowMapper());
		return data;
	}

	@Override
	public List<String> mappedIdLookup(Class pclass, Class sclass, String pid) {
		List<MappedData> mdlist = mappedLookup(pclass, sclass, pid);
		List<String> ids = new ArrayList<String>();
		for(MappedData md : mdlist) {
			ids.add(md.getSid());
		}
		return ids;
	}
	
	@Override
	public List<String> mappedIdLookup(Class pclass, Class sclass, String pid, int offset, int size) {
		List<MappedData> mdlist = mappedLookup(pclass, sclass, pid, offset, size);
		List<String> ids = new ArrayList<String>();
		for(MappedData md : mdlist) {
			ids.add(md.getSid());
		}
		return ids;
	}

	@Override
	public <Z> List<Z> indexBeanLookup(Class<Z> cls, String field, Object value) {
		ClassIndex index = indexByKeys(cls, Arrays.asList(field));
		if(index == null)
			throw new RuntimeException("index doesn't exist for " + cls + "." + field);
		List<String> ids = new ArrayList<>();
		for(IndexedData id: super.indexedLookup(null, index,Collections.singletonMap(field, value))){
			ids.add(id.getId());
		}
		return objectLookup(cls, ids);
	}

	@Override
	public <Z> List<Z> indexBeanLookup(Class<Z> cls, String field, Object value, int dataSourceId) {
		ClassIndex index = indexByKeys(cls, Arrays.asList(field));
		if(index == null)
			throw new RuntimeException("index doesn't exist for " + cls + "." + field);
		List<String> ids = new ArrayList<>();
		for(IndexedData id: super.indexedLookup(dataSourceId, index,Collections.singletonMap(field, value))){
			ids.add(id.getId());
		}
		return objectLookup(cls, ids);
	}
	
	@Override
	public <Z> List<Z> indexBeanLookup(Class<Z> cls, String field, String indexedId, int offset, int size) {
		ClassIndex index = indexByKeys(cls, Arrays.asList(field));
		ExtendedDataSource dataSource = shardedDataSource.getDataSourceByObjectId(indexedId);
		NamedParameterJdbcTemplate namedjc = dataSource.getNamedParameterJdbcTemplate();
		String sql = "select * from " + index.getTableName() + " where " + new ColumnPath(field).getColumnName() + " = :field order by created desc limit " + offset + ", " + size;
		List<IndexedData> data = namedjc.query(sql.toString(), Collections.singletonMap("field", indexedId), new IndexedDataRowMapper());
		List<String> ids = new ArrayList<>();
		for(IndexedData id: data){
			ids.add(id.getId());
		}
		return objectLookup(cls, ids);
	}
	
	@Override
	public <Z> List<Z> indexBeanLikeLookup(Class<Z> cls, String field, String indexedId, int offset, int size) {
		ClassIndex index = indexByKeys(cls, Arrays.asList(field));
		ExtendedDataSource dataSource = shardedDataSource.getDataSourceByObjectId(indexedId);
		NamedParameterJdbcTemplate namedjc = dataSource.getNamedParameterJdbcTemplate();
		String sql = "select * from " + index.getTableName() + " where " + new ColumnPath(field).getColumnName() + " like :field order by created desc limit " + offset + ", " + size;
		List<IndexedData> data = namedjc.query(sql.toString(), Collections.singletonMap("field", indexedId), new IndexedDataRowMapper());
		List<String> ids = new ArrayList<>();
		for(IndexedData id: data){
			ids.add(id.getId());
		}
		return objectLookup(cls, ids);
	}

	@Override
	public <Z> List<Z> indexBeanLookup(Class<Z> cls,
			Map<String, Object> keyValues) {
		ClassIndex index = indexByKeys(cls, keyValues.keySet());
		List<String> ids = new ArrayList<>();
		for(IndexedData id: super.indexedLookup(null, index,keyValues)){
			ids.add(id.getId());
		}
		return objectLookup(cls, ids);
	}

	@Override
	public <T> Map<String, T> mapBeans(Class<T> cls, List<String> ids) {
		Map<String, T> map = new HashMap<String, T>();
		List<T> list = objectLookup(cls, ids);
		for(T od : list) {
		    try {
                String id = (String) ReflectionUtil.getFieldValue(od, "id");
                map.put(id, od);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
		}
		return map;
	}

	@Override
	public <T> List<T> listBeans(Class<T> cls, List<String> ids) {
		return objectLookup(cls, ids);
	}

	@Override
	public <T> void forEachBean(final Class<T> cls, final BeanHandler<T> handler) {
		ShardRunnable runnable = new ShardRunnable() {
			@Override
			public void run(int dataSourceId) {
				ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(dataSourceId);
				String sql = "select * from " + cls.getSimpleName();
				JdbcTemplate jt = dataSource.getJdbcTemplate();
			    final BeanManager<?> beanManager = getTableManager().getTable(cls).getBeanManager();
				jt.query(sql, new RowCallbackHandler() {
					@Override
					public void processRow(ResultSet rs) throws SQLException {
						try {
							T t = (T) beanManager.getRowMapper().mapRow(rs, -1);
							handler.processBean(t);
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					}
				});
			}
		};
		forEachDataSourceOneByOne(runnable );
	}

	@Override
	public <Z> List<Z> indexBeanLookup(Class<Z> cls,
			Map<String, Object> keyValues, int dataSourceId) {
		return indexBeanLookup(cls, keyValues, -1, -1, dataSourceId);
	}

	@Override
	public <Z> List<Z> indexBeanLookup(Class<Z>cls, String sql,
			Map<String, Object> keyValues, int dataSourceId) {
		List<IndexedData> data = super.indexLookup(dataSourceId, sql.toString(), keyValues);
		
		List<String> ids = new ArrayList<>();
		for(IndexedData id: data){
			ids.add(id.getId());
		}
		return objectLookup(cls, ids);
	}

	@Override
	public <Z> List<Z> indexBeanLookup(Class<Z>cls, String sql,
			Map<String, Object> keyValues) {
		List<IndexedData> data = super.indexLookup(sql.toString(), keyValues);
		
		List<String> ids = new ArrayList<>();
		for(IndexedData id: data){
			ids.add(id.getId());
		}
		return objectLookup(cls, ids);
	}

	@Override
	public void indexBeanDelete(Class cls,
			Map<String, Object> keyValues,
			int dataSourceId) {
		List list = indexBeanLookup(cls, keyValues, -1, -1, dataSourceId);
		removeBeans(list);
	}

	@Override
	public <Z> List<Z> indexBeanLookup(Class<Z> cls,
			Map<String, Object> keyValues, int offset, int size,
			int dataSourceId) {
		ClassIndex index = indexByKeys(cls, keyValues.keySet());
		final StringBuilder sql = new StringBuilder("select * from ").append(index.getTableName());
		boolean first = true;
		Boolean emptyCollection = null;
		Map<String, Object> params = new HashMap<String, Object>(keyValues);
		for(String s : keyValues.keySet()) {
			if(first) {
				sql.append(" where ");
				first = false;
			}else {
				sql.append(" and ");
			}
			
			Object value = keyValues.get(s);
			if(value == null) {
				sql.append(s).append(" is null ");
			}
			else if(value instanceof Collection) {
				sql.append(s).append(" in (:").append(s).append(")");
				Collection c = (Collection) value;
				emptyCollection = c.size() == 0;
			}
			else if(value instanceof ObjectOperation) {
				ObjectOperation oo = (ObjectOperation) value;
				sql.append(s).append(oo.operation).append(" :").append(s);
				params.put(s, oo.obj);
			}
			else {
				sql.append(s).append("=:").append(s);
			}
		}
		sql.append(" order by created desc ");
		if(offset>=0)
			sql.append(" limit :offset,:size");
		
		if(offset>=0) {
			params.put("offset", offset);
			params.put("size", size);
		}

		List<IndexedData> data = null;
		if(Boolean.TRUE.equals(emptyCollection)) 
			data = new ArrayList<IndexedData>();
		else
			data = super.indexLookup(dataSourceId, sql.toString(), params);
		
		List<String> ids = new ArrayList<>();
		for(IndexedData id: data){
			ids.add(id.getId());
		}
		return objectLookup(cls, ids);
	}

	@Override
	public List<Class> listManagedClasses() {
		return forClasses;
	}
    
    @Override
    public <T> List<T> queryBeans(final String sql, final Class<T>cls, int dataSourceId) {
        ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(dataSourceId);
        JdbcTemplate jt = dataSource.getJdbcTemplate();
        List<T> result = (List<T>) jt.query(sql, getTableManager().getTable(cls).getBeanManager().getRowMapper());
        return result;
    }
	
	@Override
	public <T> List<T> queryBeans(final String sql, final Class<T>cls) {
		final List<T> list = new ArrayList<T>();
		forEachDataSource(new ShardRunnable() {
			@Override
			public void run(int dataSourceId) {
				ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(dataSourceId);
				JdbcTemplate jt = dataSource.getJdbcTemplate();
		        List<T> result = (List<T>) jt.query(sql, getTableManager().getTable(cls).getBeanManager().getRowMapper());
				synchronized (list) {
					list.addAll(result);
				}
			}
		});
		return list;
	}

	@Override
	public <T> T indexLookupForOne(Class<T> cls, String field, Object value) {
		List<T> list = indexLookup(cls, field, value);
		return list.size() == 0 ? null : list.get(0);
	}

	@Override
	public <T> T indexLookupForOne(Class<T> cls, Map<String, Object> keyValues) {
		List<T> list = indexLookup(cls, keyValues);
		return list.size() == 0 ? null : list.get(0);
	}

	@Override
	public <T> T indexLookupForOne(Class<T> cls, String field, Object value,
			int dataSourceId) {
		List<T> list = indexLookup(cls, field, value, dataSourceId);
		return list.size() == 0 ? null : list.get(0);
	}

	@Override
	public <T> T indexLookupForOne(Class<T> cls,
			Map<String, Object> keyValues, int dataSourceId) {
		List<T> list = indexLookup(cls, keyValues, dataSourceId);
		return list.size() == 0 ? null : list.get(0);
	}

	@Override
	public <Z> Z indexBeanLookupForOne(Class<Z> cls, String field, Object value) {
		List<Z> list = indexBeanLookup(cls, field, value);
		return list.size() == 0 ? null : list.get(0);
	}

	@Override
	public <Z> Z indexBeanLookupForOne(Class<Z> cls, String field,
			Object value, int dataSourceId) {
		List<Z> list = indexBeanLookup(cls, field, value, dataSourceId);
		return list.size() == 0 ? null : list.get(0);
	}

	@Override
	public <Z> Z indexBeanLookupForOne(Class<Z> cls,
			Map<String, Object> keyValues) {
		List<Z> list = indexBeanLookup(cls, keyValues);
		return list.size() == 0 ? null : list.get(0);
	}

	@Override
	public <T> T indexLookupForOne(Class<T> cls, String field, String id,
			int offset, int size) {
		List<T> list = indexLookup(cls, field, id, offset, size);
		return list.size() == 0 ? null : list.get(0);
	}

	@Override
	public <Z> Z indexBeanLookupForOne(Class<Z> cls, String field, String id,
			int offset, int size) {
		List<Z> list = indexBeanLookup(cls, field, id, offset, size);
		return list.size() == 0 ? null : list.get(0);
	}

	@Override
	public <Z> Z indexBeanLookupForOne(Class<Z> cls,
			Map<String, Object> keyValues, int offset, int size,
			int dataSourceId) {
		List<Z> list = indexBeanLookup(cls, keyValues, offset, size, dataSourceId);
		return list.size() == 0 ? null : list.get(0);
	}

	@Override
	public <Z> Z indexBeanLookupForOne(Class<Z> cls,
			Map<String, Object> keyValues, int dataSourceId) {
		List<Z> list = indexBeanLookup(cls, keyValues, dataSourceId);
		return list.size() == 0 ? null : list.get(0);
	}

	@Override
	public <Z> Z indexBeanLikeLookupForOne(Class<Z> cls, String field,
			String id, int offset, int size) {
		List<Z> list = indexBeanLikeLookup(cls, field, id, offset, size);
		return list.size() == 0 ? null : list.get(0);
	}

	@Override
	public <Z> Z indexBeanLookupForOne(Class<Z>cls, String sql,
			Map<String, Object> keyValues, int dataSourceId) {
		List<Z> list = indexBeanLookup(cls, sql, keyValues, dataSourceId);
		return list.size() == 0 ? null : list.get(0);
	}

	@Override
	public <Z> Z indexBeanLookupForOne(Class<Z>cls, String sql, Map<String, Object> keyValues) {
		List<Z> list = indexBeanLookup(cls, sql, keyValues);
		return list.size() == 0 ? null : list.get(0);
	}

    @Override
    public int getNumberOfObjects(Class cls, TimeRange tr) {
            String sql = "select count(*) from " + cls.getSimpleName();
            
            if(tr != null) {
                boolean first = true;
                if(tr.startYear != null) {
                    Calendar dayOfMonth = DateUtil.dayOfMonth(tr.startYear, tr.startMonth, tr.startDay);
                    if(first) {
                        first = false;
                        sql += " where ";
                    }
                    else 
                        sql += " and ";
                    sql += " created >= " + dayOfMonth.getTimeInMillis();
                }
                    
                if(tr.endYear != null) {
                    Calendar dayOfMonth = DateUtil.dayOfMonth(tr.endYear, tr.endMonth, tr.endDay);
                    if(first) {
                        first = false;
                        sql += " where ";
                    }
                    else 
                        sql += " and ";
                    sql += " created < " + dayOfMonth.getTimeInMillis();
                }
            }
                    
            return indexCountLookup(sql, null);
    }
}
