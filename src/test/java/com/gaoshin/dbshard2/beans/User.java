package com.gaoshin.dbshard2.beans;

import com.gaoshin.dbshard2.ClassTable;
import com.gaoshin.dbshard2.JsonBeanManager;

public class User {
	public String id;
	public long created;
	public String name;
	
	public static ClassTable table = new ClassTable(User.class, null, null, 
			new JsonBeanManager<>(User.class));
}
