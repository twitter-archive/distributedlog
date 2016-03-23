package com.twitter.distributedlog.function;

import scala.runtime.AbstractFunction1;

import java.util.List;

public class VoidFunctions {

    public static AbstractFunction1<List<Void>, Void> LIST_TO_VOID_FUNC =
            new AbstractFunction1<List<Void>, Void>() {
                @Override
                public Void apply(List<Void> list) {
                    return null;
                }
            };

}
