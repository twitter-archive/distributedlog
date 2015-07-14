package com.twitter.distributedlog.exceptions;

import com.twitter.distributedlog.thrift.service.StatusCode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;

/**
 * TODO: move ZKException to distributedlog-protocol
 */
public class ZKException extends DLException {

    private static final long serialVersionUID = 7542748595054923600L;

    final KeeperException.Code code;

    public ZKException(String msg, Code code) {
        super(StatusCode.ZOOKEEPER_ERROR, msg + " : " + code);
        this.code = code;
    }

    public ZKException(String msg, KeeperException exception) {
        super(StatusCode.ZOOKEEPER_ERROR, msg, exception);
        this.code = exception.code();
    }

    public Code getKeeperExceptionCode() {
        return this.code;
    }

    public static boolean isRetryableZKException(ZKException zke) {
        KeeperException.Code code = zke.getKeeperExceptionCode();
        return KeeperException.Code.CONNECTIONLOSS == code ||
                KeeperException.Code.OPERATIONTIMEOUT == code ||
                KeeperException.Code.SESSIONEXPIRED == code ||
                KeeperException.Code.SESSIONMOVED == code;
    }
}