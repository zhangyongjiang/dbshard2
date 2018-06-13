package com.bsci.dbshard2.beans;

import com.bsci.dbshard2.ClassTable;
import com.bsci.dbshard2.JsonBeanManager;

public class User {
	public String id;
	public long created;
	public String name;
	
	public static ClassTable table = new ClassTable(User.class, null, null, 
			new JsonBeanManager<>(User.class));
}
