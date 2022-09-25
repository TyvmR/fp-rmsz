package myflink;

import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Copyright (C)
 * Author:   longzhonghua
 * Email:    363694485@qq.com
 */
public class CustomTypeInfoFactory extends TypeInfoFactory <CustomTuple>{


    @Override
    public TypeInformation<CustomTuple> createTypeInfo(Type t, Map<String, TypeInformation<?>> genericParameters) {
        return new CustomTupleTypeInfo(genericParameters.get("T0"), genericParameters.get("T1"));
    }
}

