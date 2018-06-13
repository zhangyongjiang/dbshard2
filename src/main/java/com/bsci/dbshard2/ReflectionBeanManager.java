package com.bsci.dbshard2;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.bsci.dbshard2.impl.ReflectionRowMapper;
import common.util.reflection.FieldFoundCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import common.util.JacksonUtil;
import common.util.reflection.ReflectionUtil;

public class ReflectionBeanManager<T> extends BeanManagerBase<T>{
    private Map<DbDialet, List<String>> createSqls = new HashMap<>();

    public ReflectionBeanManager(Class<T> cls) {
    	super(cls);
	}
    
	@Override
	public int createBean(final T obj, JdbcTemplate template) {
        String id = getId(obj);
		final ObjectId oi = new ObjectId(id);
		final AtomicInteger ups = new AtomicInteger();
		
        final StringBuilder sql = new StringBuilder();
        final StringBuilder valuesSql = new StringBuilder();
        final List<Object> values = new ArrayList<>();

        try {
            ReflectionUtil.iterateFields(obj.getClass(), obj, new FieldFoundCallback() {
                @Override
                public void field(Object o, Field field) throws Exception {
                    if(!Modifier.isStatic(field.getModifiers()) && !Modifier.isTransient(field.getModifiers())) {
                        if(sql.length() > 0) {
                            sql.append(", ");
                            valuesSql.append(",");
                        }
                        sql.append(field.getName());
                        valuesSql.append("?");
                        Class<?> type = field.getType();
                        Object fieldValue = field.get(obj);
                        if(fieldValue == null)
                            values.add(null);
                        else if(type.isEnum()) {
                            values.add(fieldValue.toString());
                        }
                        else if (String.class.equals(type))
                            values.add(fieldValue);
                        else if (ReflectionUtil.isPrimeType(type)) {
                            values.add(fieldValue);
                        }
                        else {
                            String json = JacksonUtil.obj2Json(fieldValue);
                            values.add(json);
                        }
                    }
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        sql.insert(0, "insert into " + obj.getClass().getSimpleName() + " (");
        valuesSql.insert(0, " values (");
		sql.append(")");
		valuesSql.append(")");
		
		int res = template.update(sql.toString() + valuesSql.toString(), values.toArray());
		ups.getAndAdd(res);
		return ups.get();
	}

	@Override
	public int updateBean(final T obj, JdbcTemplate jt) {
        String id = (String) ReflectionUtil.getFieldValue(obj, "id");
		final ObjectId oi = new ObjectId(id);
		final AtomicInteger ups = new AtomicInteger();
		
        final StringBuilder sql = new StringBuilder();
        final List<Object> values = new ArrayList<>();

        try {
            ReflectionUtil.iterateFields(obj.getClass(), obj, new FieldFoundCallback() {
                @Override
                public void field(Object o, Field field) throws Exception {
                    if(!Modifier.isStatic(field.getModifiers()) && !Modifier.isTransient(field.getModifiers())) {
                        if(sql.length() > 0) {
                            sql.append(", ");
                        }
                        sql.append(field.getName()).append("=?");
                        
                        Class<?> type = field.getType();
                        Object fieldValue = field.get(obj);
                        if(type.isEnum()) {
                        	if(fieldValue == null)
                        		values.add(null);
                        	else
                        		values.add(fieldValue.toString());
                        }
                        else if (String.class.equals(type))
                            values.add(fieldValue);
                        else if (ReflectionUtil.isPrimeType(type)) {
                            values.add(fieldValue);
                        }
                        else {
                            String json = JacksonUtil.obj2Json(fieldValue);
                            values.add(json);
                        }
                    }
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        sql.insert(0, "update " + obj.getClass().getSimpleName() + " set ");
        sql.append(" where id=?");
        values.add(id);
        
		int res = jt.update(sql.toString(), values.toArray());
		ups.getAndAdd(res);
		return ups.get();
	}

	@Override
	public T get(String id, JdbcTemplate tempalte) {
		T data = null;
		if(id != null){
				ObjectId oi = new ObjectId(id);
				String sql = "select * from " + cls.getSimpleName() + " where id=?";
				List<T> od = tempalte.query(sql, new Object[]{id}, new ReflectionRowMapper(cls));
				
				data = od.size() > 0 ? od.get(0) : null;
		}
		return data;
	}

	@Override
	public List<String> getCreateSqls(DbDialet dialet) {
		List<String> list = createSqls.get(dialet);
		if(list == null)
			list = createSqls.get(null);
		return list;
	}

	public ReflectionBeanManager setCreateSqls(DbDialet dialet, List<String> createSqls) {
		this.createSqls.put(dialet, createSqls);
		return this;
	}

	@Override
	public RowMapper<T> getRowMapper() {
		return new ReflectionRowMapper<>(getForClass());
	}
}
