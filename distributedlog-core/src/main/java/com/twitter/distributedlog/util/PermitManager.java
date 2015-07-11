package com.twitter.distributedlog.util;

public interface PermitManager {

    public static interface Permit {
        static final Permit ALLOWED = new Permit() {
            @Override
            public boolean isAllowed() {
                return true;
            }
        };
        boolean isAllowed();
    }

    public static PermitManager UNLIMITED_PERMIT_MANAGER = new PermitManager() {
        @Override
        public Permit acquirePermit() {
            return Permit.ALLOWED;
        }

        @Override
        public void releasePermit(Permit permit) {
            // nop
        }

        @Override
        public boolean allowObtainPermits() {
            return true;
        }

        @Override
        public boolean disallowObtainPermits(Permit permit) {
            return false;
        }

    };

    /**
     * Obetain a permit from permit manager.
     *
     * @return permit.
     */
    Permit acquirePermit();

    /**
     * Release a given permit.
     *
     * @param permit
     *          permit to release
     */
    void releasePermit(Permit permit);

    /**
     * Allow obtaining permits.
     */
    boolean allowObtainPermits();

    /**
     * Disallow obtaining permits. Disallow needs to be performed under the context
     * of <i>permit</i>.
     *
     * @param permit
     *          permit context to disallow
     */
    boolean disallowObtainPermits(Permit permit);
}
