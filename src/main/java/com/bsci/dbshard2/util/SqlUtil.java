/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bsci.dbshard2.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Collection;
import java.util.HashMap;

import javax.persistence.Transient;

import com.bsci.dbshard2.util.reflection.FieldFoundCallback;
import com.bsci.dbshard2.util.reflection.ReflectionUtil;

public class SqlUtil {
    public static boolean ignoreSupported = true;
    
    public static String getInsertStatement(Collection<?> list, boolean insertIgnore) throws Exception {
        String ret = null;
        if(list.size()>0){
            final StringBuilder sb = new StringBuilder();
            for(Object obj : list){
                if(sb.length()==0){
                    sb.append(getInsertHeader(obj,insertIgnore));
                }else{
                    sb.append(",");
                }
                sb.append(getInsertValues(obj));
            }
            ret = sb.toString();
        }
        return ret;
    }
    
    public static String getInsertStatement(Object obj) throws Exception {
        return getInsertHeader(obj)+getInsertValues(obj);
    }
    
    public static String getInsertIgnoreStatement(Object obj) throws Exception {
        return getInsertHeader(obj, true)+getInsertValues(obj);
    }
    
    public static String getInsertHeader(Object obj, boolean insertIgnore) throws Exception {
        final StringBuilder sb = new StringBuilder();
        Class<?> beanClass = obj.getClass();
        ReflectionUtil.iterateFields(beanClass, obj, new FieldFoundCallback() {
            @Override
            public void field(Object o, Field field) throws Exception {
                if((Modifier.STATIC & field.getModifiers()) == Modifier.STATIC || ReflectionUtil.annotatedWith(field, Transient.class)) {
                    return;
                }
                if(sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(CamelUnderScore.underscore(field.getName()));
            }
        });
        String table = CamelUnderScore.underscore(obj.getClass().getSimpleName());
        if(insertIgnore && ignoreSupported)
            return "insert ignore into "+table+" ("+sb.toString()+") VALUES ";
        else
            return "insert into "+table+" ("+sb.toString()+") VALUES ";
    }
    
    public static String getInsertValues(Object obj) throws Exception {
        final StringBuilder sb = new StringBuilder();
        Class<?> beanClass = obj.getClass();
        ReflectionUtil.iterateFields(beanClass, obj, new FieldFoundCallback() {
            @Override
            public void field(Object o, Field field) throws Exception {
                field.setAccessible(true);
                if((Modifier.STATIC & field.getModifiers()) == Modifier.STATIC || ReflectionUtil.annotatedWith(field, Transient.class)) {
                    return;
                }
                Object fieldValue = field.get(o);
                if(sb.length() > 0) {
                    sb.append(", ");
                }
                Class<?> fieldType = field.getType();
                if(String.class.equals(fieldType) && fieldValue != null) {
                    String value = ((String)fieldValue).replaceAll("'", "''").replaceAll("\\\\", "\\\\\\\\");
                    sb.append("'"+value+"'");
                }
                else if(fieldType.isEnum() && fieldValue != null) {
                    sb.append("'"+fieldValue+"'");
                }
                else {
                    sb.append(fieldValue);
                }
            }
        });
        return "("+sb.toString()+")";
    }
    
    public static String getUpdateStatement(Object obj, String idFieldName) throws Exception {
        final StringBuilder sb = new StringBuilder();
        Class<?> beanClass = obj.getClass();
        ReflectionUtil.iterateFields(beanClass, obj, new FieldFoundCallback() {
            @Override
            public void field(Object o, Field field) throws Exception {
                field.setAccessible(true);
                Object fieldValue = field.get(o);
                if(fieldValue != null && !ReflectionUtil.annotatedWith(field, Transient.class)){
                    if(sb.length() > 0) {
                        sb.append(", ");
                    }
                    sb.append(CamelUnderScore.underscore(field.getName()));
                    sb.append("=");
                    Class<?> fieldType = field.getType();
                    if(String.class.equals(fieldType)) {
                        String value = ((String)fieldValue).replaceAll("'", "''").replaceAll("\\\\", "\\\\\\\\");
                        sb.append("'"+value+"'");
                    }
                    else if(fieldType.isEnum()) {
                        sb.append("'"+fieldValue+"'");
                    }
                    else {
                        sb.append(fieldValue);
                    }
                }
            }
        });
        String table = CamelUnderScore.underscore(obj.getClass().getSimpleName());
        String id = (String) ReflectionUtil.getFieldValue(obj, idFieldName);
        sb.append(" where ").append(CamelUnderScore.underscore(idFieldName)).append("='").append(id).append("'");
        return "update " + table + " set " + sb.toString();
    }
    
    public static String getDeleteStatement(Object obj, String idFieldName) throws Exception {
        final StringBuilder sb = new StringBuilder();
        String table = CamelUnderScore.underscore(obj.getClass().getSimpleName());
        String id = (String) ReflectionUtil.getFieldValue(obj, idFieldName);
        sb.append("delete from "+table);
        sb.append(" where ").append(CamelUnderScore.underscore(idFieldName)).append("='").append(id).append("'");
        return sb.toString();
    }
    
    public static String getDeleteStatement(Class<?> clazz, String idFieldName, String in){
        String table = CamelUnderScore.underscore(clazz.getSimpleName());
        String field = CamelUnderScore.underscore(idFieldName);
        return "delete from "+table+" where "+field+" = '"+in+"'";
    }

    public static String getDeleteStatementIsn(Class<?> clazz, String idFieldName, String in){
        String table = CamelUnderScore.underscore(clazz.getSimpleName());
        String field = CamelUnderScore.underscore(idFieldName);
        return "delete from "+table+" where "+field+" in ("+in+")";
    }
    public static String getInsertStatement(Collection<?> list) throws Exception {
        return getInsertStatement(list, false);
    }
    
    public static String getInsertIgnoreStatement(Collection<?> list) throws Exception {
        return getInsertStatement(list, true);
    }
    
    public static String getInsertHeader(Object obj) throws Exception {
        return getInsertHeader(obj, false);
    }

    public static String join(String[] ids) {
        return join(",",ids);
    }

    public static String join(Collection<String> ids) {
        return join(",",ids);
    }
    
    public static String join(String delimeter, String[] ids){
        StringBuffer sb = new StringBuffer(ids.length * 8);
        for (String s : ids) {
            sb.append(delimeter).append("'").append(s).append("'");
        }
        return sb.substring(ids.length > 0 ? delimeter.length() : 0);
    }
    
    public static String join(String delimeter, Collection<String> ids){
        StringBuffer sb = new StringBuffer();
        for (String s : ids) {
            sb.append(delimeter).append("'").append(s).append("'");
        }
        return sb.substring(ids.size() > 0 ? delimeter.length() : 0);
    }
    
    public static <T> T resultSetToEntity(Class<T> clazz, ResultSet rs){
        T t = null;
        try{
            final HashMap<String,String> rowVals = new HashMap<String,String>();
            ResultSetMetaData rsmd = rs.getMetaData();
            for(int i = 1; i<= rsmd.getColumnCount(); i++){
                rowVals.put(rsmd.getColumnName(i),rs.getString(i));
            }
            t = clazz.newInstance();
            ReflectionUtil.iterateFields(t, new FieldFoundCallback() {
                @Override
                public void field(Object obj, Field field) throws Exception {
                    field.setAccessible(true);
                    String name = CamelUnderScore.underscore(field.getName());
                    if((Modifier.STATIC & field.getModifiers()) != Modifier.STATIC){
                        String fieldValue = rowVals.get(name);
                        if(fieldValue != null){
                            try{
                                field.set(obj, ReflectionUtil.convert(fieldValue, field.getType()));
                            }catch(Exception e){
                                e.printStackTrace();
                            }
                        }
                    }
                }
            });
        }catch(Exception e){
            throw new RuntimeException(e);
        }
        return t;
    }
}
