package com.gaoshin.dbshard2;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import common.util.DateUtil;

public class JsonBeanManager<T> extends BeanManagerBase<T> {
	private static ObjectMapper objectMapper = new ObjectMapper();
	
	public JsonBeanManager(Class<T> cls) {
		super(cls);
	}

	@Override
	public int createBean(T obj, JdbcTemplate template) {
		String id = getId(obj);
		Long created = getCreated(obj);
		if(created == null)
			created = DateUtil.currentTimeMillis();
		long updated = created;
		String json = null;
		try {
			json = objectMapper.writeValueAsString(obj);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
		final AtomicInteger ups = new AtomicInteger();
		String sql = "insert into " + cls.getSimpleName() + " (`id`, `created`, `updated`, `json`) values (?, ?, ?, ?)";
		int res = template.update(sql, id, created, updated, json);
		ups.getAndAdd(res);
		return ups.get();
	}

	@Override
	public int updateBean(T obj, JdbcTemplate template) {
		String id = getId(obj);
		long updated = DateUtil.currentTimeMillis();
		String json = null;
		try {
			json = objectMapper.writeValueAsString(obj);
		} catch (JsonProcessingException e) {
			throw new RuntimeException(e);
		}
		final AtomicInteger ups = new AtomicInteger();
		String sql = "update " + cls.getSimpleName() + " set updated=?, json=? where id=?)";
		int res = template.update(sql, updated, json, id);
		ups.getAndAdd(res);
		return ups.get();
	}

	@Override
	public T get(String id, JdbcTemplate tempalte) {
		String sql = "select json from " + cls.getSimpleName() + " where id=?";
		List<String> json = tempalte.queryForList(sql, new Object[]{id}, String.class);
		if(json.size() == 0)
			return null;
		try {
			T t = objectMapper.readValue(json.get(0), getForClass());
			return t;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<String> getCreateSqls(DbDialet dbdialet) {
		if(DbDialet.Mysql.equals(dbdialet)) {
		    return Collections.singletonList("create table if not exists `" + cls.getSimpleName() + "` (`id` varchar(64) primary key, `created` bigint, `updated` bigint, `json` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin)");
		}
		else if(DbDialet.H2.equals(dbdialet)) {
			return Collections.singletonList("create table if not exists `" + cls.getSimpleName() + "` (`id` varchar(64) primary key, `created` bigint, `updated` bigint, `json` text)");
		}
		else {
			return Collections.singletonList("create table if not exists `" + cls.getSimpleName() + "` (`id` varchar(64) primary key, `created` bigint, `updated` bigint, `json` text)");
		}
	}

	@Override
	public RowMapper<T> getRowMapper() {
		return new JsonRowMapper<>();
	}

	class JsonRowMapper<T> implements RowMapper<T> {

		@Override
		public T mapRow(ResultSet rs, int rowNum) throws SQLException {
			try {
				String json = rs.getString("json");
				return (T) objectMapper.readValue(json, getForClass());
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		
	}

}
