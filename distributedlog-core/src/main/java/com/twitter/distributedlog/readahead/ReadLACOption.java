package com.twitter.distributedlog.readahead;

public enum ReadLACOption {
    DEFAULT (0), LONGPOLL(1), READENTRYPIGGYBACK_PARALLEL (2), READENTRYPIGGYBACK_SEQUENTIAL(3), INVALID_OPTION(4);
    private final int value;

    private ReadLACOption(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
