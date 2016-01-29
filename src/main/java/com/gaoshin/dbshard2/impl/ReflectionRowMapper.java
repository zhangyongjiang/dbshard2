package com.gaoshin.dbshard2.impl;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

import common.util.reflection.ReflectionUtil;

public class ReflectionRowMapper<T> implements RowMapper<T> {
    private Class<T> cls;
    
    public ReflectionRowMapper(Class<T> cls) {
        this.cls = cls;
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
                Field field = cls.getField(label);
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
