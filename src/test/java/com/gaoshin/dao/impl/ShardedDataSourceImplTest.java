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

package com.gaoshin.dao.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.gaoshin.dao.ExtendedDataSource;
import com.gaoshin.dao.ObjectId;
import com.gaoshin.dao.ShardedDataSourceImpl;
import com.gaoshin.dao.RequestContext;

public class ShardedDataSourceImplTest {
	@Test
	public void test(){
		RequestContext tc = new RequestContext();
		
		ShardedDataSourceImpl sds = new ShardedDataSourceImpl();
		sds.setDbClassName("org.h2.Driver");
		sds.setUserName("sa");
		sds.setUrl("jdbc:h2:mem:test__DATASOURCEID__;MODE=MySQL;DB_CLOSE_ON_EXIT=FALSE");
		sds.setShardsPerDataSource(8);
		
		List<String> ids = new ArrayList<String>();
		for(int shardId=0; shardId<65; shardId++) {
			ExtendedDataSource eds = sds.getDataSourceByShardId(tc, shardId);
			Assert.assertEquals(shardId/8, eds.getDataSourceId());
			Assert.assertEquals("jdbc:h2:mem:test" + (shardId/8) + ";MODE=MySQL;DB_CLOSE_ON_EXIT=FALSE", eds.getUrl());
			
			ObjectId objectId = new ObjectId(shardId);
			objectId.setType("usr");
			ids.add(objectId.toString());
		}
		
		Map<Integer, List<String>> map = sds.splitByDataSource(ids);
		Assert.assertEquals(9, map.size());
		for(int i=0; i<9; i++) {
			Assert.assertNotNull(map.get(i));
			List<String> list = map.get(i);
			if(i==8)
				Assert.assertEquals(1, list.size());
			else
				Assert.assertEquals(8, list.size());
		}
		
		for(int dataSourceId=0; dataSourceId<65; dataSourceId++) {
			ExtendedDataSource eds = sds.getDataSourceByDataSourceId(tc, dataSourceId);
			Assert.assertEquals(dataSourceId, eds.getDataSourceId());
			Assert.assertEquals("jdbc:h2:mem:test" + dataSourceId + ";MODE=MySQL;DB_CLOSE_ON_EXIT=FALSE", eds.getUrl());
		}
	}
}
