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

public class ClassIndex {
	public Class forClass;
	public String[] columns;
	public boolean unique;
	
	private String tableName;
	
	public ClassIndex(Class forClass, String[] columns) {
		this(forClass, columns, false);
	}
	
	public ClassIndex(Class forClass, String[] columns, boolean unique) {
		this.forClass = forClass;
		this.columns = columns;
		this.unique = unique;
	}
	
	public String getTableName() {
		if(tableName != null)
			return tableName;
		
		StringBuilder sb = new StringBuilder();
		sb.append("i_");
		sb.append(forClass.getSimpleName());
		for(String column : columns) {
			sb.append("_");
			String columnName = new ColumnPath(column).getColumnName();
			sb.append(columnName);
		}
		tableName = sb.toString();
		if(tableName.length()>64)
			tableName = tableName.substring(0, 63);
		return tableName;
	}
}
