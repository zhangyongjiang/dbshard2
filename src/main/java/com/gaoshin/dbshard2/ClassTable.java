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

import java.util.HashMap;

public class ClassTable {
	private Class forcls;
	private ClassIndex[] indexes;
	private ClassMapping[] mappings;
	private HashMap<DbDialet, String> tableCreateSqls = new HashMap<>();

	public Class getForcls() {
		return forcls;
	}

	public ClassTable(Class forcls, ClassIndex[] indexes,
			ClassMapping[] mappings) {
		this.forcls = forcls;
		this.indexes = indexes;
		this.mappings = mappings;
	}

	public ClassMapping getClassMapping(Class cls) {
		for (ClassMapping mapping : getMappings()) {
			if (mapping.map2cls.equals(cls))
				return mapping;
		}
		throw new RuntimeException(forcls.getSimpleName()
				+ " has no mapping for " + cls);
	}

	public ClassIndex[] getIndexes() {
		return indexes;
	}

	public ClassMapping[] getMappings() {
		return mappings;
	}

	public ClassTable addCreateSql(DbDialet dialet, String sql) {
		synchronized (tableCreateSqls) {
			tableCreateSqls.put(dialet, sql);
			return this;
		}
	}

	public String getCreateSql(DbDialet dialet) {
		synchronized (tableCreateSqls) {
			return tableCreateSqls.get(dialet);
		}
	}

}
