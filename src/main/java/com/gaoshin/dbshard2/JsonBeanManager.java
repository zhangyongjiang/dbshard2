package com.gaoshin.dbshard2;

import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;

public class JsonBeanManager<T> implements BeanManager<T> {

	@Override
	public int createBean(T obj, JdbcTemplate template) {
		return 0;
	}

	@Override
	public int updateBean(T obj, JdbcTemplate template) {
		return 0;
	}

	@Override
	public T get(String id, JdbcTemplate tempalte, Class<?> cls) {
		return null;
	}

	@Override
	public List<String> getCreateSqls(DbDialet dialet) {
		// TODO Auto-generated method stub
		return null;
	}

}
