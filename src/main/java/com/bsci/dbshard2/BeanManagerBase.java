package com.bsci.dbshard2;

import common.util.reflection.ReflectionUtil;

public abstract class BeanManagerBase<T> implements BeanManager<T> {
	protected Class<T> cls;
	
	public BeanManagerBase(Class<T> cls) {
		this.cls = cls;
	}
	
    public static String getId(Object o) {
        return (String) ReflectionUtil.getFieldValue(o, "id");
    }
    
    public static Long getCreated(Object o) {
        return (Long) ReflectionUtil.getFieldValue(o, "created");
    }

	@Override
	public Class<T> getForClass() {
		return cls;
	}

}
