package com.twitter.distributedlog.messaging;

/**
 * Transform one format to the other format.
 */
public interface Transformer<T, R> {

    /**
     * Transform value <i>T</i> to value <i>R</i>. If null is returned,
     * the value <i>T</i> will be eliminated without transforming.
     *
     * @param value value to transform
     * @return transformed value
     */
    R transform(T value);

}
