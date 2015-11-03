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

package com.gaoshin.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;

public class TableManagerForPackage extends TableManager {
	private static final Logger logger = Logger.getLogger(TableManagerForPackage.class);
	
	public TableManagerForPackage(String pkgName) throws IOException, ClassNotFoundException {
		ImmutableSet<ClassInfo> classes = ClassPath.from(this.getClass().getClassLoader()).getTopLevelClasses(pkgName);
		List<Class> list = new ArrayList<Class>();
		for(ClassInfo ci : classes) {
			Class<?> clz = Class.forName(ci.getName());
			ShardedTable annotation = clz.getAnnotation(ShardedTable.class);
			if(annotation == null) {
				logger.info(clz + " is not annotated with ShardedTable. Skip it.");
				continue;
			}
			list.add(clz);
		}
		addClasses(list);
	}
}
