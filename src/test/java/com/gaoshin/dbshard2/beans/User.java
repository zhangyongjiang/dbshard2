package com.gaoshin.dbshard2.beans;

import com.gaoshin.dbshard2.ClassTable;

public class User {
	public String id;
	public long created;
	public long updated;
	public String name;
	
	public static ClassTable table = new ClassTable(User.class, null, null);
}
