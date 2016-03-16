package com.gaoshin.dbshard2.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;

import com.gaoshin.dbshard2.DbDialet;
import com.gaoshin.dbshard2.H2InMemoryShardedDataSource;
import com.gaoshin.dbshard2.ShardResolver;
import com.gaoshin.dbshard2.ShardedDataSource;
import com.gaoshin.dbshard2.TableManager;
import com.gaoshin.dbshard2.beans.User;

public class ExtendedDaoImplTest {
	@Test
	public void testOneShard() {
		ExtendedDaoImpl dao = new ExtendedDaoImpl();
		
		ExecutorService executorService = Executors.newFixedThreadPool(1);
		dao.executorService = executorService;
		
		TableManager manager = new TableManager();
		manager.addTable(User.table);
		dao.tableManager = manager;
		
		ShardResolver shardResolver = new SingleShardResolver();
		dao.shardResolver = shardResolver;
		
		ShardedDataSource dataSource = new H2InMemoryShardedDataSource("testOneShard");
		dao.shardedDataSource = dataSource;
		
		dao.addClass(User.class);
		dao.createTables(DbDialet.H2);
		
		User user = new User();
		user.name = "name";
		
		dao.createBean(user);
	}
}
