package myflink;

import org.apache.flink.api.common.typeinfo.TypeInfo;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
@TypeInfo(CustomTypeInfoFactory.class)
public class CustomTuple<T0, T1> {
    public T0 field0;
    public T1 field1;
}
