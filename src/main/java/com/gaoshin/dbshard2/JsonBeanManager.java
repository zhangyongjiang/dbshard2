package com.gaoshin.dbshard2;

import java.util.Collections;
import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;

public class JsonBeanManager<T> implements BeanManager<T> {
	private Class<T> cls;
	
	public JsonBeanManager(Class<T> cls) {
		this.cls = cls;
	}

	@Override
	public int createBean(T obj, JdbcTemplate template) {
		return 0;
	}

	@Override
	public int updateBean(T obj, JdbcTemplate template) {
		return 0;
	}

	@Override
	public T get(String id, JdbcTemplate tempalte) {
		return null;
	}

	@Override
	public List<String> getCreateSqls(DbDialet dbdialet) {
		if(DbDialet.Mysql.equals(dbdialet)) {
		    return Collections.singletonList("create table if not exists `" + cls.getSimpleName() + "` (`id` varchar(64) primary key, `created` bigint, `updated` bigint, `version` integer, `json` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin");
		}
		else if(DbDialet.H2.equals(dbdialet)) {
			return Collections.singletonList("create table if not exists `" + cls.getSimpleName() + "` (`id` varchar(64) primary key, `created` bigint, `updated` bigint, `version` integer, `json` text");
		}
		else {
		    throw new RuntimeException("unsupported db dialet");
		}
	}

	@Override
	public Class<T> getForClass() {
		return cls;
	}

}
