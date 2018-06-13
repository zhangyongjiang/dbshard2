package com.bsci.dbshard2.beans;

import com.bsci.dbshard2.ClassIndex;
import com.bsci.dbshard2.ClassMapping;
import com.bsci.dbshard2.ClassTable;
import com.bsci.dbshard2.JsonBeanManager;
import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;

public class Account {
	public String id;
	public long created;
	public String userId;
	public String extId;
	public String type;

	@Override
	public int hashCode() {
		return (int) Hashing.md5()
				.hashString(type + "/" + extId, Charsets.UTF_8).asLong();
	}

	public static ClassTable table = new ClassTable(Account.class,
			new ClassIndex[] {
				new ClassIndex(Account.class, new String[]{"extId", "type"})
			}, 
			new ClassMapping[] {
				new ClassMapping(Account.class, "userId", User.class, null)
	},
	new JsonBeanManager<>(Account.class));

}
