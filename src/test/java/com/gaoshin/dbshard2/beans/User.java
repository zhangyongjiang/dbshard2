package com.gaoshin.dbshard2.beans;

import com.gaoshin.dbshard2.ClassTable;

public class User {
	public String id;
	public long created;
	public String name;
	
	public static ClassTable table = new ClassTable(User.class, null, null)
			.addCreateSql(null, "create table User (id varchar(64) primary key, created bigint, name varchar(64))");
}
