package com.gaoshin.dbshard2.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.Test;

import com.gaoshin.dbshard2.H2InMemoryShardedDataSource;
import com.gaoshin.dbshard2.ObjectId;
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
		dao.updateAll(User.table.getCreateSql(null));

		User user = new User();
		user.name = "name";

		dao.createBean(user);
		Assert.assertNotNull(user.id);

		ObjectId oi = new ObjectId(user.id);
		Assert.assertEquals(0, oi.getShard());
		Assert.assertTrue(user.created > 0);

		User db = dao.objectLookup(User.class, user.id);
		Assert.assertEquals(user.name, db.name);
		Assert.assertEquals(user.id, db.id);
		Assert.assertEquals(user.created, db.created);
	}

	@Test
	public void testMultipleShard() {
		int numberOfShards = 16;
		ExtendedDaoImpl dao = new ExtendedDaoImpl();

		ExecutorService executorService = Executors.newFixedThreadPool(32);
		dao.executorService = executorService;

		TableManager manager = new TableManager();
		manager.addTable(User.table);
		dao.tableManager = manager;

		ShardResolverBase shardResolver = new ShardResolverBase<>();
		shardResolver.setNumberOfShards(numberOfShards);
		shardResolver.setMinShardId4Write(0);
		shardResolver.setMaxShardId4Write(numberOfShards);
		dao.shardResolver = shardResolver;

		ShardedDataSource dataSource = new H2InMemoryShardedDataSource("shard16");
		dao.shardedDataSource = dataSource;

		dao.addClass(User.class);
		dao.updateAll(User.table.getCreateSql(null));

		final Map<String, User> map = new HashMap<String, User>();
		int loop = 1000;
		for (int i = 0; i < loop; i++) {
			User user = new User();
			user.name = "name" + i;

			dao.createBean(user);
			map.put(user.name, user);
		}
		
		dao.forEachBean(User.class, new BeanHandler<User>() {
			@Override
            public void processBean(User bean) {
				map.remove(bean.name);
            }
		});
		Assert.assertEquals(0, map.size());
	}
}
