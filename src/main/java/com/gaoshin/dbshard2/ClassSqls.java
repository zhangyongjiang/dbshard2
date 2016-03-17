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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class ClassSqls {
	public Class forcls;
	public List<String> sqls = new ArrayList<>();
	
	public void addSql(String sql) {
		if(!sqls.contains(sql))
			sqls.add(sql);
	}
	
	public void addAllSqls(List<String> sqls) {
		for(String sql : sqls)
			addSql(sql);
	}
	
	public static void a2b(Map<Class, ClassSqls> a, Map<Class, ClassSqls> b) {
		for(Entry<Class, ClassSqls> entry : a.entrySet()) {
			Class cls = entry.getKey();
			ClassSqls classSqls = b.get(cls);
			if(classSqls == null) {
				b.put(cls, entry.getValue());
			}
			else {
				classSqls.addAllSqls(entry.getValue().sqls);
			}
		}
	}
}
