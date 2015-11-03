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

import common.util.UuidUtil;


public class ObjectId {
	private int shard;
	private String uuid;
	
	public ObjectId() {
		uuid = UuidUtil.randomType1Uuid();
	}
	
	public ObjectId(int shardId) {
		this.shard = shardId;
		uuid = UuidUtil.randomType1Uuid();
	}
	
	public ObjectId(Class cls, int shardId) {
		this.shard = shardId;
		uuid = UuidUtil.randomType1Uuid();
	}
	
	public ObjectId(Class cls, int shardId, String uuid) {
		this.shard = shardId;
		this.uuid = uuid;
	}
	
	public ObjectId(String strid) {
		try {
			setShard(Short.parseShort(strid.substring(3,6), 16));
			setUuid(strid.substring(6));
		} catch (Exception e) {
			throw new InvalidIdException(strid);
		}
	}
	
	@Override
	public String toString() {
		return String.format("%03x%s", shard, uuid);
	}
	
	public int getShard() {
		return shard;
	}
	
	public static short getShard(String id) {
		return Short.parseShort(id.substring(3, 6), 16);
	}

	public void setShard(int shard) {
		this.shard = shard;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}
	
}
