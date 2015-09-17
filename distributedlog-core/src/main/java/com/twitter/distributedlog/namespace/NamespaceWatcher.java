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
