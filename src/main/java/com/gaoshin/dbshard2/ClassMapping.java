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

public class ClassMapping {
	public Class forClass;
	public String column;
	public Class map2cls;
	public String[] otherColumns;
	
	private String tableName;
	
	public ClassMapping() {
	}
	
	public ClassMapping(Class forClass, String column, Class map2Cls, String[] otherColumns) {
		this.forClass = forClass;
		this.column = column;
		this.map2cls = map2Cls;
		this.otherColumns = otherColumns;
	}
	
	public String getTableName() {
		if(tableName != null)
			return tableName;
		
		StringBuilder sb = new StringBuilder();
		sb.append("m_");
		sb.append(map2cls.getSimpleName());
		sb.append("_");
		sb.append(forClass.getSimpleName());
		tableName = sb.toString();
		
		return tableName;
	}
}
