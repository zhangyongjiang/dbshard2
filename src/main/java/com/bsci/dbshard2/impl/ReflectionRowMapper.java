package com.bsci.dbshard2.impl;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.springframework.jdbc.core.RowMapper;

import com.bsci.dbshard2.util.reflection.FieldFoundCallback;
import com.bsci.dbshard2.util.reflection.ReflectionUtil;

public class ReflectionRowMapper<T> implements RowMapper<T> {
	private static Map<Class, Map<String, Field>> clsFields = new HashMap<>();
	private static void addClass(final Class cls) {
		synchronized(clsFields) {
			Map<String, Field> fields = clsFields.get(cls);
			if(fields != null) {
				return;
			}
			fields = new HashMap<>();
			clsFields.put(cls, fields);
			
			final Map<String, Field> tmp = fields;
			try {
				ReflectionUtil.iterateFields(cls, null, new FieldFoundCallback() {
					@Override
					public void field(Object o, Field field) throws Exception {
	                    if(!Modifier.isStatic(field.getModifiers()) && !Modifier.isTransient(field.getModifiers())) {
	                    	field.setAccessible(true);
	                    	tmp.put(field.getName().toUpperCase(), field);
	                    }
					}
				});
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
	private static Field getClassField(final Class cls, String name) {
		synchronized(clsFields) {
			Map<String, Field> fields = clsFields.get(cls);
			if(fields == null) {
				return null;
			}
			return fields.get(name.toUpperCase());
		}
	}
	
    private Class<T> cls;
    
    public ReflectionRowMapper(Class<T> cls) {
        this.cls = cls;
        addClass(cls);
    }

    @Override
    public T mapRow(ResultSet rs, int rowNum) throws SQLException {
        return mapRow(rs);
    }

    public T mapRow(ResultSet rs) throws SQLException {
        try {
            T t = cls.newInstance();
            ResultSetMetaData metaData = rs.getMetaData();
            int cnt = metaData.getColumnCount();
            for(int i=1; i<=cnt; i++) {
                String label = metaData.getColumnLabel(i);
                Field field = getClassField(cls, label);
                field.setAccessible(true);
                String str = rs.getString(i);
                Object value = ReflectionUtil.convert(str, field);
                field.set(t, value);
            }
            return t;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
