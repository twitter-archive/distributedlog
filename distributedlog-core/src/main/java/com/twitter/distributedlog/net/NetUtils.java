package com.twitter.distributedlog.net;

import org.apache.bookkeeper.net.DNSToSwitchMapping;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Utils about network
 */
public class NetUtils {

    /**
     * Get the dns resolver from class <code>resolverClassName</code> with optional
     * <code>hostRegionOverrides</code>.
     * <p>
     * It would try to load the class with the constructor with <code>hostRegionOverrides</code>.
     * If it fails, it would fall back to load the class with default empty constructor.
     * The interpretion of <code>hostRegionOverrides</code> is up to the implementation.
     *
     * @param resolverCls
     *          resolver class
     * @param hostRegionOverrides
     *          host region overrides
     * @return dns resolver
     */
    public static DNSToSwitchMapping getDNSResolver(Class<? extends DNSToSwitchMapping> resolverCls,
                                                    String hostRegionOverrides) {
        // first try to construct the dns resolver with overrides
        Constructor<? extends DNSToSwitchMapping> constructor;
        Object[] parameters;
        try {
            constructor = resolverCls.getDeclaredConstructor(String.class);
            parameters = new Object[] { hostRegionOverrides };
        } catch (NoSuchMethodException nsme) {
            // no constructor with overrides
            try {
                constructor = resolverCls.getDeclaredConstructor();
                parameters = new Object[0];
            } catch (NoSuchMethodException nsme1) {
                throw new RuntimeException("Unable to find constructor for dns resolver "
                        + resolverCls, nsme1);
            }
        }
        constructor.setAccessible(true);
        try {
            return constructor.newInstance(parameters);
        } catch (InstantiationException ie) {
            throw new RuntimeException("Unable to instantiate dns resolver " + resolverCls, ie);
        } catch (IllegalAccessException iae) {
            throw new RuntimeException("Illegal access to dns resolver " + resolverCls, iae);
        } catch (InvocationTargetException ite) {
            throw new RuntimeException("Unable to construct dns resolver " + resolverCls, ite);
        }
    }

}
