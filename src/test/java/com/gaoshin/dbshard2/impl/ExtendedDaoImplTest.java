package com.gaoshin.dbshard2.impl;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Assert;
import org.junit.Test;

import com.gaoshin.dbshard2.ClassSqls;
import com.gaoshin.dbshard2.H2InMemoryShardedDataSource;
import com.gaoshin.dbshard2.ObjectId;
import com.gaoshin.dbshard2.RequestContext;
import com.gaoshin.dbshard2.ShardResolver;
import com.gaoshin.dbshard2.ShardedDataSource;
import com.gaoshin.dbshard2.TableManager;
import com.gaoshin.dbshard2.beans.Account;
import com.gaoshin.dbshard2.beans.User;

public class ExtendedDaoImplTest {
	
	@Test
	public void testOneShard() {
		ExtendedDaoImpl dao = new ExtendedDaoImpl();

		ExecutorService executorService = Executors.newFixedThreadPool(1);
		dao.executorService = executorService;

		TableManager manager = new TableManager();
		manager.addTable(User.table);
		dao.setTableManager(manager);

		ShardResolver shardResolver = new SingleShardResolver();
		dao.shardResolver = shardResolver;

		ShardedDataSource dataSource = new H2InMemoryShardedDataSource();
		dao.shardedDataSource = dataSource;

		dao.addClass(User.class);
		dao.updateSqls(dao.getCreateTableSqls(null));

		User user = new User();
		user.name = "name";

		dao.createBean(user);
		Assert.assertNotNull(user.id);

		ObjectId oi = new ObjectId(user.id);
		Assert.assertEquals(0, oi.getShard());
		Assert.assertTrue(user.created > 0);

		User db = dao.getObject(User.class, user.id);
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
		dao.setTableManager(manager);

		ShardResolverBase shardResolver = new ShardResolverBase<>();
		shardResolver.setNumberOfShards(numberOfShards);
		shardResolver.setMinShardId4Write(0);
		shardResolver.setMaxShardId4Write(numberOfShards);
		dao.shardResolver = shardResolver;

		ShardedDataSource dataSource = new H2InMemoryShardedDataSource();
		dao.shardedDataSource = dataSource;

		dao.addClass(User.class);
		dao.updateSqls(dao.getCreateTableSqls(null));

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

	@Test
	public void testMinMaxShardIdForWrite() {
		final int numberOfShards = 16;
		final int min = 8;
		final int max = 13;
		
		ExtendedDaoImpl dao = new ExtendedDaoImpl();

		ExecutorService executorService = Executors.newFixedThreadPool(32);
		dao.executorService = executorService;

		TableManager manager = new TableManager();
		manager.addTable(User.table);
		dao.setTableManager(manager);

		ShardResolverBase shardResolver = new ShardResolverBase<>();
		shardResolver.setNumberOfShards(numberOfShards);
		shardResolver.setMinShardId4Write(min);
		shardResolver.setMaxShardId4Write(max);
		dao.shardResolver = shardResolver;

		ShardedDataSource dataSource = new H2InMemoryShardedDataSource();
		dao.shardedDataSource = dataSource;

		dao.addClass(User.class);
		dao.updateSqls(dao.getCreateTableSqls(null));

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
				ObjectId oi = new ObjectId(bean.id);
				Assert.assertTrue(oi.getShard() >= min);
				Assert.assertTrue(oi.getShard() < max);
            }
		});
	}
	
	@Test
	public void testTransaction() throws SQLException {
		RequestContext rc = new RequestContext();
		RequestContext.setRequestContext(rc);
		
		ExtendedDaoImpl dao = new ExtendedDaoImpl();

		ExecutorService executorService = Executors.newFixedThreadPool(1);
		dao.executorService = executorService;

		TableManager manager = new TableManager();
		manager.addTable(User.table);
		dao.setTableManager(manager);

		ShardResolver shardResolver = new SingleShardResolver();
		dao.shardResolver = shardResolver;

		ShardedDataSource dataSource = new H2InMemoryShardedDataSource();
		dao.shardedDataSource = dataSource;

		dao.addClass(User.class);
		dao.updateSqls(dao.getCreateTableSqls(null));

		{
			User user = new User();
			user.name = "name";
	
			dao.createBean(user);
			Assert.assertNotNull(user.id);
			
			rc.commit();
			User db = dao.getObject(User.class, user.id);
			Assert.assertNotNull(db);
		}

		{
			User user = new User();
			user.name = "name";
	
			dao.createBean(user);
			Assert.assertNotNull(user.id);
			
			rc.rollback();
			User db = dao.getObject(User.class, user.id);
			Assert.assertNull(db);
		}
	}
	
	@Test
	public void testIndexAndMapping() {
		TableManager manager = new TableManager();
		Map<Class, ClassSqls> createTableSqls;
		
		ExtendedDaoImpl userDao = new ExtendedDaoImpl();
		{
			int numberOfShards = 4;
	
			ExecutorService executorService = Executors.newFixedThreadPool(32);
			userDao.executorService = executorService;
	
			manager.addTable(User.table);
			userDao.setTableManager(manager);
	
			ShardResolverBase shardResolver = new ShardResolverBase<>();
			shardResolver.setNumberOfShards(numberOfShards);
			shardResolver.setMinShardId4Write(0);
			shardResolver.setMaxShardId4Write(numberOfShards);
			userDao.shardResolver = shardResolver;
	
			ShardedDataSource dataSource = new H2InMemoryShardedDataSource();
			userDao.shardedDataSource = dataSource;

			userDao.addClass(User.class);
			createTableSqls = userDao.getCreateTableSqls(null);
		}
		
		ExtendedDaoImpl accountDao = new ExtendedDaoImpl();
		{
			int numberOfShards = 16;
	
			ExecutorService executorService = Executors.newFixedThreadPool(32);
			accountDao.executorService = executorService;
	
			manager.addTable(Account.table);
			accountDao.setTableManager(manager);
	
			FixedShardResolver shardResolver = new FixedShardResolver<>();
			shardResolver.setNumberOfShards(numberOfShards);
			accountDao.shardResolver = shardResolver;
	
			ShardedDataSource dataSource = new H2InMemoryShardedDataSource();
			accountDao.shardedDataSource = dataSource;

			accountDao.addClass(Account.class);
			Map<Class, ClassSqls> accTables = accountDao.getCreateTableSqls(null);
			ClassSqls.a2b(accTables, createTableSqls);
		}
		
		userDao.updateSqls(createTableSqls);
		accountDao.updateSqls(createTableSqls);
		
		User user = new User();
		userDao.createBean(user);
		
		Account acc0 = new Account();
		acc0.extId = "0";
		acc0.type = "t0";
		acc0.userId = user.id;
		accountDao.createBean(acc0);
		
		Account acc1 = new Account();
		acc1.extId = "1";
		acc1.type = "t0";
		acc1.userId = user.id;
		accountDao.createBean(acc1);
		
		{
			Map<String, Object> params = new HashMap<>();
			params.put("extId", acc0.extId);
			params.put("type", acc0.type);
			List<Account> list = accountDao.indexBeanLookup(Account.class, params);
			Assert.assertEquals(1, list.size());
		}
		
		{
			Map<String, Object> params = new HashMap<>();
			params.put("type", acc0.type);
			List<Account> list = accountDao.indexBeanLookup(Account.class, params);
			Assert.assertEquals(2, list.size());
		}
		
		{
			List<String> list = userDao.mappedIdLookup(User.class, Account.class, user.id);
			Assert.assertEquals(2, list.size());
		}
	}

}
