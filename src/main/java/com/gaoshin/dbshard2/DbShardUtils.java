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

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;

import org.apache.log4j.Logger;

import common.util.reflection.ReflectionUtil;

public class DbShardUtils {
	private static final Logger logger = Logger.getLogger(DbShardUtils.class);
	
	public static Map<Class, ClassSqls> getSqls(final ClassTable classTable, DbDialet dbdialet) {
		Map<Class, ClassSqls> sqls = new HashMap<>();
		final Class<?> beanCls = classTable.getForcls();

		String createSql = classTable.getCreateSql(dbdialet);
		if(createSql == null)
		    throw new RuntimeException("don't know how to create table " + classTable.getForcls());
		
		ClassSqls thiscls = new ClassSqls();
		thiscls.forcls = classTable.getForcls();
		thiscls.addSql(createSql);
		logger.debug(classTable.getForcls() + ": " + createSql);
		sqls.put(thiscls.forcls, thiscls);
		
		ClassIndex[] indexes = classTable.getIndexes();
		if(indexes != null) {
			for(ClassIndex index : indexes) {
				String[] columns = index.columns;
				String tableName = index.getTableName();
				StringBuilder sb = new StringBuilder();
				StringBuilder indexTable = new StringBuilder();
				sb.append("create table if not exists ").append(tableName).append("(id varchar(64)");
				indexTable.append("alter table ").append(tableName).append(" add ").append(index.unique ? " unique " : "").append(" index i").append(tableName).append("(");
				boolean first = true;
				boolean hasCreated = false;
				for(String c : columns) {
					ColumnPath cp = new ColumnPath(c);
					String columnName = cp.getColumnName();
					if("created".equals(columnName))
						hasCreated = true;
					sb.append(",");
					sb.append(columnName).append(" ").append(getColumnType(beanCls, c));
					
					if(first) {
						first = false;
					}
					else {
						indexTable.append(",");
					}
					indexTable.append(columnName);
				}
				if(!hasCreated)
					sb.append(", created bigint)");
				else 
					sb.append(")");
				indexTable.append(")");
				
				String sql = sb.toString();
				thiscls.addSql(sql);
				logger.debug(thiscls.forcls + ": " + sql);
				
				sql = indexTable.toString();
				thiscls.addSql(sql);
				logger.debug(thiscls.forcls + ": " + sql);
				
				sql = "alter table " + tableName + " add index idindex (id)";
				thiscls.addSql(sql);
				logger.debug(thiscls.forcls + ": " + sql);
			}
		}
		
		ClassMapping[] mappings = classTable.getMappings();
		if(mappings != null) {
			for(ClassMapping mapping : mappings) {
				Class map2cls = mapping.map2cls;
				ClassSqls map2sqls = sqls.get(map2cls);
				if(map2sqls == null) {
					map2sqls = new ClassSqls();
					map2sqls.forcls = map2cls;
					sqls.put(map2cls, map2sqls);
				}
				
				StringBuilder table = new StringBuilder();
				table.append("create table if not exists ").append(mapping.getTableName()).append("(pid varchar(64), sid varchar(64), created bigint");
				if(mapping.otherColumns != null) {
					for(String c : mapping.otherColumns) {
						ColumnPath cp = new ColumnPath(c);
						String columnName = cp.getColumnName();
						if("created".equals(columnName))
							continue;
						table.append(",");
						table.append(columnName).append(" ").append(getColumnType(beanCls, c));
					}
				}
				table.append(")");
				String sql = table.toString();
				map2sqls.addSql(sql);
				logger.debug(map2cls + ": " + sql);
				
				sql = "alter table " + mapping.getTableName() + " add index sidindex (sid)";
				map2sqls.addSql(sql);
				logger.debug(map2cls + ": " + sql);
				
				sql = "alter table " + mapping.getTableName() + " add index pidindex (pid)";
				map2sqls.addSql(sql);
				logger.debug(map2cls + ": " + sql);
			}
		}
		
		return sqls;
	}

	private static String getColumnType(Class<?> beanCls, String columnName) {
		try {
			int pos = columnName.indexOf(".");
			if(pos != -1) {
				String fieldName = columnName.substring(0, pos);
				Field field = ReflectionUtil.getField(beanCls, fieldName);
				Class<?> fieldType = field.getType();
				if(fieldType.isAssignableFrom(List.class)) {
					Class<?> listItemType = ReflectionUtil.getFieldGenericType(field);
					return getColumnType(listItemType, columnName.substring(pos+1));
				}
				else {
					return getColumnType(fieldType, columnName.substring(pos+1));
				}
			}
			
			Field field = ReflectionUtil.getField(beanCls, columnName);
			Class<?> type = field.getType();
			if(type.isAssignableFrom(List.class)) {
				// handle List<String> use case
				type = ReflectionUtil.getFieldGenericType(field);
			}
			
	        if (type.equals(String.class)) {
				try {
					Column colAnno = (Column)field.getAnnotation(Column.class);
					if(colAnno != null) {
						String definition = colAnno.columnDefinition();
						if(definition != null && definition.length() > 0)
							return definition;
					    int length = colAnno.length();
					    if(length > 0)
							return "varchar(" + length + ")";
					}
				} catch (Throwable e) {
//					throw new RuntimeException(e);
				}
				return "varchar(64)";
	        }
	        
	        if (type.equals(Integer.class) || type.equals(int.class)) {
	            return "integer";
	        }
	        if (type.equals(Float.class) || type.equals(float.class)) {
	            return "float";
	        }
	        if (type.equals(Double.class) || type.equals(double.class)) {
	            return "float";
	        }
	        if (type.equals(Long.class) || type.equals(long.class)) {
	            return "bigint";
	        }
	        if (type.equals(Boolean.class) || type.equals(boolean.class)) {
	            return "integer";
	        }
	        if (type.equals(Date.class)) {
	            return "bigint";
	        }
	        if (type.equals(Calendar.class)) {
	            return "bigint";
	        }
	        if (type.equals(BigInteger.class)) {
	            return "bigint";
	        }
	        if (type.equals(BigDecimal.class)) {
	            return "bigint";
	        }
	        if(type.isEnum()) {
	            return "varchar(64)";
	        }
			
			return null;
		} catch (Exception e) {
			throw new RuntimeException(beanCls + ":" + columnName, e);
		}
	}
	
	public static Map getMap(Object... keyValues) {
		Map map = new HashMap();
		if(keyValues == null)
			return map;
		for(int i=0; i<keyValues.length; i+=2) {
			map.put(keyValues[i], keyValues[i+1]);
		}
		return map;
	}
}
