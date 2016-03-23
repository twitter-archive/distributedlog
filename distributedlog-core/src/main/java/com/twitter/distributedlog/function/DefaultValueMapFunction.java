package com.twitter.distributedlog.function;

import scala.runtime.AbstractFunction1;

/**
 * Map Function return default value
 */
public class DefaultValueMapFunction<T, R> extends AbstractFunction1<T, R> {

    public static <T, R> DefaultValueMapFunction<T, R> of(R defaultValue) {
        return new DefaultValueMapFunction<T, R>(defaultValue);
    }

    private final R defaultValue;

    private DefaultValueMapFunction(R defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Override
    public R apply(T any) {
        return defaultValue;
    }
}
