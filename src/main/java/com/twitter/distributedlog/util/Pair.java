package com.twitter.distributedlog.util;

public class Pair<F, L> {

    final F first;
    final L last;

    private Pair(F first, L last) {
        this.first = first;
        this.last = last;
    }

    public F getFirst() {
        return first;
    }

    public L getLast() {
        return last;
    }

    public static <F, L> Pair<F, L> of(F first, L last) {
        return new Pair<F, L>(first, last);
    }
}
