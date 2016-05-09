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
package com.twitter.distributedlog.namespace;

import com.twitter.distributedlog.callback.NamespaceListener;

import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Namespace Watcher watching namespace changes.
 */
public abstract class NamespaceWatcher {

    protected final CopyOnWriteArraySet<NamespaceListener> listeners =
            new CopyOnWriteArraySet<NamespaceListener>();

    /**
     * Register listener for namespace changes.
     *
     * @param listener
     *          listener to add
     */
    public void registerListener(NamespaceListener listener) {
        if (listeners.add(listener)) {
            watchNamespaceChanges();
        }
    }

    /**
     * Unregister listener from the namespace watcher.
     *
     * @param listener
     *          listener to remove from namespace watcher
     */
    public void unregisterListener(NamespaceListener listener) {
        listeners.remove(listener);
    }

    /**
     * Watch the namespace changes. It would be triggered each time
     * a namspace listener is added. The implementation should handle
     * this.
     */
    protected abstract void watchNamespaceChanges();

}
