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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.gaoshin.dbshard2.entity.MappedData;
import com.gaoshin.dbshard2.impl.BeanHandler;

public interface ExtendedDao extends BaseDao {
	Map<Class, ClassSqls> getCreateTableSqls(DbDialet dialet);
	void updateSqls(Map<Class, ClassSqls> sqls);

	List<Class> listManagedClasses();
	void createBean(Object obj);
    void updateBeanAndIndexAndMapping(Object obj);
	int delete(Class cls, String id);
	int deleteAll(Class cls, Collection<String> ids);
	int deleteBean(Object od);
	<T> Map<String, T> mapBeans(Class<T> cls, List<String> ids);
	void removeBeans(List list);
    <T> List<T> queryBeans(final String sql, Class<T>cls);
    <T> List<T> queryBeans(final String sql, Class<T>cls, int dataSourceId);

	<T> List<T> indexLookup(Class<T> cls, String field, Object value);
	<T> List<T> indexLookup(Class<T> cls, Map<String, Object> keyValues);
	<T> List<T> indexLookup(Class<T> cls, String field, Object value, int dataSourceId);
	<T> List<T> indexLookup(Class<T> cls, Map<String, Object> keyValues, int dataSourceId);
	int indexCountLookup(Class cls, String field, Object value, int dataSourceId);
	int indexCountLookup(Class cls, Map<String, Object> keyValues, int dataSourceId);
	int indexCountLookup(Class cls, String field, Object value);
	int indexCountLookup(Class cls, Map<String, Object> keyValues);
	<Z>List<Z> indexBeanLookup(Class<Z>cls, String field, Object value);
	<Z>List<Z> indexBeanLookup(Class<Z>cls, String field, Object value, int dataSourceId);
	<Z>List<Z> indexBeanLookup(Class<Z>cls, Map<String, Object> keyValues);
	<T> List<T> indexLookup(Class<T> cls, String field, String id, int offset, int size);
	<Z>List<Z> indexBeanLookup(Class<Z>cls, String field, String id, int offset, int size);
	<Z>List<Z> indexBeanLookup(Class<Z>cls, Map<String, Object> keyValues, int offset, int size, int dataSourceId);
	<Z>List<Z> indexBeanLookup(Class<Z>cls, Map<String, Object> keyValues, int dataSourceId);
	<Z>List<Z> indexBeanLikeLookup(Class<Z>cls, String field, String id, int offset, int size);
	<Z> List<Z> indexBeanLookup(Class<Z>cls, String sql, Map<String, Object> keyValues, int dataSourceId);
	<Z> List<Z> indexBeanLookup(Class<Z>cls, String sql, Map<String, Object> keyValues);
	
	<T> T indexLookupForOne(Class<T> cls, String field, Object value);
	<T> T indexLookupForOne(Class<T> cls, Map<String, Object> keyValues);
	<T> T indexLookupForOne(Class<T> cls, String field, Object value, int dataSourceId);
	<T> T indexLookupForOne(Class<T> cls, Map<String, Object> keyValues, int dataSourceId);
	<Z> Z indexBeanLookupForOne(Class<Z>cls, String field, Object value);
	<Z> Z indexBeanLookupForOne(Class<Z>cls, String field, Object value, int dataSourceId);
	<Z> Z indexBeanLookupForOne(Class<Z>cls, Map<String, Object> keyValues);
	<T> T indexLookupForOne(Class<T> cls, String field, String id, int offset, int size);
	<Z> Z indexBeanLookupForOne(Class<Z>cls, String field, String id, int offset, int size);
	<Z> Z indexBeanLookupForOne(Class<Z>cls, Map<String, Object> keyValues, int offset, int size, int dataSourceId);
	<Z> Z indexBeanLookupForOne(Class<Z>cls, Map<String, Object> keyValues, int dataSourceId);
	<Z> Z indexBeanLikeLookupForOne(Class<Z>cls, String field, String id, int offset, int size);
	<Z> Z indexBeanLookupForOne(Class<Z>cls, String sql, Map<String, Object> keyValues, int dataSourceId);
	<Z> Z indexBeanLookupForOne(Class<Z>cls, String sql, Map<String, Object> keyValues);
	
	String generateIdForBean(Object bean);
	String generateSameShardId(String id);
	
	List<MappedData> mappedLookup(Class pclass, Class sclass, String pid);
	List<String> mappedIdLookup(Class pclass, Class sclass, String pid);
	List<MappedData> mappedLookup(Class pclass, Class sclass, String pid, int offset, int size);
	List<String> mappedIdLookup(Class pclass, Class sclass, String pid, int offset, int size);
	int mappedCountLookup(Class pclass, Class sclass, String pid);

	<T> void forEachBean(Class<T>cls, BeanHandler<T> handler);
	void touchAllBean(Class cls);
	ClassIndex indexByKeys(Class forClass, Collection<String> keys);
	ClassIndex indexByKeys(Class forClass, String... keys);
	
	int getNumberOfObjects(Class cls, TimeRange tr);
}
