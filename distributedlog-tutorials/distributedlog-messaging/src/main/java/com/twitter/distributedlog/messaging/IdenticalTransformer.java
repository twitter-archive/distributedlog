package com.twitter.distributedlog.messaging;

public class IdenticalTransformer<T> implements Transformer<T, T> {
    @Override
    public T transform(T value) {
        return value;
    }
}
