package cz.oltisgroup.keycloak.cosmosdb;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.ConsistencyLevel;
import org.jboss.logging.Logger;
import org.keycloak.models.ModelException;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shared CosmosClient manager with reference counting and keep-alive delay
 * so that short-lived provider instances do not churn TCP connections.
 */
public final class CosmosClientManager {

    private static final Logger logger = Logger.getLogger(CosmosClientManager.class);

    private static class Entry {
        final CosmosClient client;
        final AtomicInteger refCount = new AtomicInteger(1);
        // When refCount drops to zero we record releaseTime and keep the client alive for keepAliveMillis
        volatile long releaseTime = -1L;
        final long keepAliveMillis;
        Entry(CosmosClient client, long keepAliveMillis) {
            this.client = client; this.keepAliveMillis = keepAliveMillis;
        }
    }

    private static final Map<String, Entry> CLIENTS = new ConcurrentHashMap<>();

    private CosmosClientManager() {}

    private static String key(String endpoint, String db, String container, String accountKey) {
        return endpoint + "|" + db + "|" + container + "|" + (accountKey == null ? "" : Integer.toHexString(accountKey.hashCode()));
    }

    /**
     * Acquire (or create) shared client. Performs lazy cleanup of expired (idle) entries.
     * @param endpoint cosmos endpoint
     * @param key account key
     * @param db database name
     * @param container container name
     * @param keepAliveSeconds how long (seconds) to keep idle client before real close (<=0 = immediate close behavior)
     */
    public static synchronized CosmosClient acquire(String endpoint, String key, String db, String container, int keepAliveSeconds) {
        cleanupExpired();
        String composite = key(endpoint, db, container, key);
        Entry e = CLIENTS.get(composite);
        if (e != null) {
            int c = e.refCount.incrementAndGet();
            e.releaseTime = -1L; // active again
            logger.debugf("Reusing CosmosClient for %s (refCount=%d)", composite, c);
            return e.client;
        }
        long keepAliveMillis = keepAliveSeconds > 0 ? keepAliveSeconds * 1000L : 0L;
        logger.infof("Creating new shared CosmosClient for endpoint=%s db=%s container=%s (keepAlive=%ds)", endpoint, db, container, keepAliveSeconds);
        CosmosClient client = new CosmosClientBuilder()
                .endpoint(endpoint)
                .key(key)
                .consistencyLevel(ConsistencyLevel.SESSION)
                .buildClient();
        CLIENTS.put(composite, new Entry(client, keepAliveMillis));
        return client;
    }

    public static synchronized void release(String endpoint, String key, String db, String container, CosmosClient client) {
        if (client == null) return;
        String composite = key(endpoint, db, container, key);
        Entry e = CLIENTS.get(composite);
        if (e == null) {
            logger.warnf("Attempted to release unknown CosmosClient key=%s", composite);
            return;
        }
        if (!Objects.equals(e.client, client)) {
            logger.warnf("Client instance mismatch for key %s; ignoring release", composite);
            return;
        }
        int remaining = e.refCount.decrementAndGet();
        if (remaining <= 0) {
            if (e.keepAliveMillis <= 0) {
                CLIENTS.remove(composite);
                logger.infof("Closing shared CosmosClient for %s (immediate)", composite);
                safeClose(e.client);
            } else {
                e.releaseTime = System.currentTimeMillis();
                logger.debugf("Marking CosmosClient idle for %s (close in ~%d ms)", composite, e.keepAliveMillis);
            }
        } else {
            logger.debugf("Released CosmosClient for %s (refCount=%d)", composite, remaining);
        }
        cleanupExpired();
    }

    private static void cleanupExpired() {
        long now = System.currentTimeMillis();
        Iterator<Map.Entry<String, Entry>> it = CLIENTS.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Entry> mapEntry = it.next();
            Entry e = mapEntry.getValue();
            if (e.refCount.get() == 0 && e.releaseTime > 0 && e.keepAliveMillis > 0 && now - e.releaseTime >= e.keepAliveMillis) {
                logger.infof("Closing shared CosmosClient for %s (idle timeout exceeded)", mapEntry.getKey());
                it.remove();
                safeClose(e.client);
            }
        }
    }

    private static void safeClose(CosmosClient client) {
        try { client.close(); } catch (Exception ex) { logger.warn("Error closing CosmosClient", ex); throw new ModelException("Error closing CosmosClient", ex);
        }
    }
}
