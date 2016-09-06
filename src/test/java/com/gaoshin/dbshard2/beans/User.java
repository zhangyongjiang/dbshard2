package com.gaoshin.dbshard2.beans;

import java.util.Collections;

import com.gaoshin.dbshard2.ClassTable;
import com.gaoshin.dbshard2.ReflectionBeanManager;

public class User {
	public String id;
	public long created;
	public String name;
	
	public static ClassTable table = new ClassTable(User.class, null, null, new ReflectionBeanManager<>().setCreateSqls(null, Collections.singletonList("create table if not exists User (id varchar(64) primary key, created bigint, name varchar(64))")));
}
