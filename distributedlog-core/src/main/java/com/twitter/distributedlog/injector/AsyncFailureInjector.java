/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.distributedlog.injector;

/**
 * Failure Injector that works in asynchronous way
 */
public interface AsyncFailureInjector {

    AsyncFailureInjector NULL = new AsyncFailureInjector() {
        @Override
        public void injectErrors(boolean enabled) {
            // no-op
        }

        @Override
        public boolean shouldInjectErrors() {
            return false;
        }

        @Override
        public void injectDelays(boolean enabled) {
            // no-op
        }

        @Override
        public boolean shouldInjectDelays() {
            return false;
        }

        @Override
        public int getInjectedDelayMs() {
            return 0;
        }

        @Override
        public void injectStops(boolean enabled) {
            // no-op
        }

        @Override
        public boolean shouldInjectStops() {
            return false;
        }

        @Override
        public boolean shouldInjectCorruption() {
            return false;
        }

        @Override
        public String toString() {
            return "NULL";
        }
    };

    /**
     * Enable or disable error injection.
     *
     * @param enabled
     *          flag to enable or disable error injection.
     */
    void injectErrors(boolean enabled);

    /**
     * Return the flag indicating if should inject errors.
     *
     * @return true to inject errors otherwise false.
     */
    boolean shouldInjectErrors();

    /**
     * Enable or disable delay injection.
     *
     * @param enabled
     *          flag to enable or disable delay injection.
     */
    void injectDelays(boolean enabled);

    /**
     * Return the flag indicating if should inject delays.
     *
     * @return true to inject delays otherwise false.
     */
    boolean shouldInjectDelays();

    /**
     * Return the injected delay in milliseconds.
     *
     * @return the injected delay in milliseconds.
     */
    int getInjectedDelayMs();

    /**
     * Enable or disable injecting stops. This could be used
     * for simulating stopping an action.
     */
    void injectStops(boolean enabled);

    /**
     * Return the flag indicating if should inject stops.
     *
     * @return true to inject stops otherwise false.
     */
    boolean shouldInjectStops();

    /**
     * Return the flag indicating if should inject corruption.
     *
     * @return true to inject corruption otherwise false.
     */
    boolean shouldInjectCorruption();
}
