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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

@SuppressWarnings({"rawtypes","unchecked"})
public class TableManager {
	private static final Logger logger = Logger.getLogger(TableManager.class);
	private List<Class> classesManaged = new ArrayList<>();
	private HashMap<Class,List<ClassIndex>> indexes = new HashMap<>();

	public TableManager(){
	}

	public TableManager(List<Class> classes){
		addClasses(classes);
	}

	public void addClasses(List<Class> classes){
		this.classesManaged.addAll(classes);
		for(Class cls : classesManaged){
			ShardedTable shardedTable = (ShardedTable) cls.getAnnotation(ShardedTable.class);
			if(shardedTable != null){
				//add to rest
				List<ClassIndex> inds = new ArrayList<>();
				indexes.put(cls, inds);
				for(Index ind : shardedTable.indexes()){
					ClassIndex ti = new ClassIndex();
					ti.forClass = cls;
					ti.index = ind;
					inds.add(ti);
				}
			}
		}
	}
	public List<Class> getClassesManaged(){
		return classesManaged;
	}
	
	public static final String getObjectDataTable(Class cls) {
		return cls.getSimpleName();
	}
	public static final String getMappedDataTable(Class pcls, Class scls) {
		return "m_" + pcls.getSimpleName() + "_" + scls.getSimpleName();
	}
	
	/*
	 * Cache key generators
	 */
	public final String getIndexedLookupKey(ClassIndex index, Map<String, ? extends Object>values) {
		StringBuilder sb = new StringBuilder();
		sb.append(index.getTableName());
		for(String c : index.index.value()) {
			sb.append(values.get(c));
		}
		return sb.toString();
	}
	public static final String getMappedLookupKey(Class clsP, Class clsS, String id) {
		return clsP.getSimpleName() + "|" + clsS.getSimpleName() + "|" + id;
	}
	public static final String getMappedIdFromKey(String key) {
		return key.substring(key.lastIndexOf("|") + 1);
	}
	
}
