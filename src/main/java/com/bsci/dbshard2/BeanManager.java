package com.bsci.dbshard2;

import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public interface BeanManager<T> {
	Class<T> getForClass();
	List<String> getCreateSqls(DbDialet dialet);
	int createBean(T obj, JdbcTemplate template);
	int updateBean(T obj, JdbcTemplate template);
	T get(String id, JdbcTemplate tempalte);
	RowMapper<T> getRowMapper();
}
