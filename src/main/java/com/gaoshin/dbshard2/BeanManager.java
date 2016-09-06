package com.gaoshin.dbshard2;

import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;

public interface BeanManager<T> {
	List<String> getCreateSqls(DbDialet dialet);
	int createBean(T obj, JdbcTemplate template);
	int updateBean(T obj, JdbcTemplate template);
	T get(String id, JdbcTemplate tempalte, Class<?>cls);
}
