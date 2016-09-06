package com.gaoshin.dbshard2;

import org.springframework.jdbc.core.JdbcTemplate;

public interface BeanManager<T> {
	int createBean(T obj, JdbcTemplate template);
	int updateBean(T obj, JdbcTemplate template);
	T get(String id, JdbcTemplate tempalte, Class<?>cls);
}
