package cz.oltisgroup.keycloak.cosmosdb;

import com.azure.cosmos.*;
import com.azure.cosmos.models.*;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.JsonNode;
import org.jboss.logging.Logger;
import org.keycloak.component.ComponentModel;
import org.keycloak.credential.CredentialInput;
import org.keycloak.credential.CredentialInputUpdater;
import org.keycloak.credential.CredentialInputValidator;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.models.UserModel;
import org.keycloak.models.credential.PasswordCredentialModel;
import org.keycloak.storage.StorageId;
import org.keycloak.storage.UserStorageProvider;
import org.keycloak.storage.user.UserLookupProvider;
import org.keycloak.storage.user.UserQueryProvider;
import org.keycloak.models.GroupModel;

import java.util.*;
import java.util.stream.Stream;

public class CosmosDbUserStorageProvider implements UserStorageProvider,
        UserLookupProvider, CredentialInputValidator, UserQueryProvider, CredentialInputUpdater {

    private static final Logger logger = Logger.getLogger(CosmosDbUserStorageProvider.class);

    private final KeycloakSession session;
    private final ComponentModel model;
    private final CosmosClient cosmosClient;
    private final CosmosContainer usersContainer;

    private final String endpoint;
    private final String key;
    private final String databaseName;
    private final String containerName;
    private final int keepAliveSeconds;

    private final Map<String, JsonNode> userDocCache = new HashMap<>();

    public CosmosDbUserStorageProvider(KeycloakSession session, ComponentModel model) {
        this.session = session;
        this.model = model;

        logger.info("Initializing CosmosDbUserStorageProvider (optimized)");

        this.endpoint = model.get(CosmosDbUserStorageProviderFactory.ENDPOINT);
        this.key = model.get(CosmosDbUserStorageProviderFactory.KEY);
        this.databaseName = model.get(CosmosDbUserStorageProviderFactory.DATABASE_NAME);
        this.containerName = model.get(CosmosDbUserStorageProviderFactory.CONTAINER_NAME);
        String keepAliveCfg = model.get(CosmosDbUserStorageProviderFactory.CLIENT_KEEP_ALIVE_SECONDS, "30");
        int ka;
        try { ka = Integer.parseInt(keepAliveCfg); } catch (NumberFormatException e) { ka = 30; }
        this.keepAliveSeconds = Math.max(0, ka);

        logger.infof("Cosmos DB Config - Endpoint: %s, Database: %s, Container: %s, KeepAlive=%ds", endpoint, databaseName, containerName, keepAliveSeconds);

        this.cosmosClient = CosmosClientManager.acquire(endpoint, key, databaseName, containerName, keepAliveSeconds);
        CosmosDatabase database = cosmosClient.getDatabase(databaseName);
        this.usersContainer = database.getContainer(containerName);

        logger.debug("CosmosDbUserStorageProvider successfully initialized (shared client)");
    }

    @Override
    public void close() {
        logger.debug("Closing CosmosDbUserStorageProvider (releasing shared client)");
        CosmosClientManager.release(endpoint, key, databaseName, containerName, cosmosClient);
        userDocCache.clear();
    }

    private boolean isUserActive(JsonNode userDoc) {
        JsonNode item = userDoc.get("Item");
        return item != null && item.has("Active") && item.get("Active").asInt() == 1;
    }

    private JsonNode cacheAndReturn(String username, JsonNode doc) {
        if (username != null && doc != null) {
            userDocCache.put(username, doc);
        }
        return doc;
    }

    private JsonNode findActiveUserByUsername(String username) {
        if (username == null) return null;
        String raw = username.trim();
        String normalized = raw.toLowerCase(Locale.ROOT);
        JsonNode cached = userDocCache.get(raw);
        if (cached == null && !raw.equals(normalized)) {
            cached = userDocCache.get(normalized);
        }
        if (cached != null) {
            return cached;
        }

        try {
            String query = "SELECT c.Header, c.Item FROM c WHERE LOWER(c.Header.UserAdId) = @uname";
            SqlQuerySpec querySpec = new SqlQuerySpec(query, Collections.singletonList(new SqlParameter("@uname", normalized)));
            CosmosPagedIterable<JsonNode> results = usersContainer.queryItems(querySpec, new CosmosQueryRequestOptions(), JsonNode.class);
            for (JsonNode userDoc : results) {
                if (isUserActive(userDoc)) {
                    JsonNode header = userDoc.get("Header");
                    String storedName = (header != null && header.has("UserAdId")) ? header.get("UserAdId").asText() : raw;
                    userDocCache.put(storedName, userDoc);
                    userDocCache.put(normalized, userDoc);
                    if (!storedName.equals(raw)) {
                        userDocCache.put(raw, userDoc);
                    }
                    return userDoc;
                }
            }
        } catch (Exception e) {
            logger.error("Error querying user by username (case-insensitive): " + raw, e);
        }
        return null;
    }

    @Override
    public UserModel getUserById(RealmModel realm, String id) {
        logger.debugf("getUserById called with id: %s", id);
        StorageId storageId = new StorageId(id);
        String username = storageId.getExternalId();
        return getUserByUsername(realm, username);
    }

    @Override
    public UserModel getUserByUsername(RealmModel realm, String username) {
        logger.debugf("getUserByUsername called with username: %s", username);
        JsonNode userDoc = findActiveUserByUsername(username);
        if (userDoc != null) {
            logger.debugf("Active user %s found (cached=%s)", username, userDocCache.containsKey(username));
            return new CosmosDbUserAdapter(session, realm, model, userDoc);
        }
        logger.debugf("No active user found for %s", username);
        return null;
    }

    @Override
    public UserModel getUserByEmail(RealmModel realm, String email) {
        logger.debugf("getUserByEmail called with email: %s", email);
        try {
            String query = "SELECT c.Header, c.Item FROM c WHERE c.Item.Email = @email";
            SqlQuerySpec querySpec = new SqlQuerySpec(query, Collections.singletonList(new SqlParameter("@email", email)));
            CosmosPagedIterable<JsonNode> results = usersContainer.queryItems(querySpec, new CosmosQueryRequestOptions(), JsonNode.class);
            for (JsonNode userDoc : results) {
                if (isUserActive(userDoc)) {
                    String username = userDoc.has("Header") && userDoc.get("Header").has("UserAdId") ? userDoc.get("Header").get("UserAdId").asText() : null;
                    cacheAndReturn(username, userDoc);
                    return new CosmosDbUserAdapter(session, realm, model, userDoc);
                }
            }
        } catch (Exception e) {
            logger.error("Error fetching user by email: " + email, e);
        }
        return null;
    }

    @Override
    public boolean supportsCredentialType(String credentialType) {
        return PasswordCredentialModel.TYPE.equals(credentialType);
    }

    @Override
    public boolean isConfiguredFor(RealmModel realm, UserModel user, String credentialType) {
        if (!supportsCredentialType(credentialType)) {
            return false;
        }
        JsonNode doc = findActiveUserByUsername(user.getUsername());
        if (doc != null) {
            JsonNode item = doc.get("Item");
            return item != null && item.has("Password") && !item.get("Password").asText().isBlank();
        }
        return false;
    }

    @Override
    public boolean isValid(RealmModel realm, UserModel user, CredentialInput input) {
        if (!supportsCredentialType(input.getType())) {
            logger.debugf("Unsupported credential type: %s", input.getType());
            return false;
        }

        String providedPassword = input.getChallengeResponse();
        if (providedPassword == null) {
            logger.debug("Provided password is null");
            return false;
        }

        JsonNode userDoc = findActiveUserByUsername(user.getUsername());
        if (userDoc == null) {
            logger.debugf("User document not found or inactive for %s", user.getUsername());
            return false;
        }

        JsonNode item = userDoc.get("Item");
        if (item == null || !item.has("Password")) {
            logger.debugf("Password field missing for user %s", user.getUsername());
            return false;
        }

        String storedPassword = item.get("Password").asText();
        boolean valid = providedPassword.equals(storedPassword);
        logger.debugf("Password validation for user %s result: %s (cacheHit=%s)", user.getUsername(), valid, userDocCache.containsKey(user.getUsername()));
        return valid;
    }

    @Override
    public int getUsersCount(RealmModel realm) {
        try {
            String query = "SELECT VALUE COUNT(1) FROM c WHERE c.Item.Active = 1";
            CosmosPagedIterable<Integer> results = usersContainer.queryItems(query, new CosmosQueryRequestOptions(), Integer.class);
            for (Integer count : results) {
                return count;
            }
        } catch (Exception e) {
            logger.error("Error getting users count", e);
        }
        return 0;
    }

    @Override
    public Stream<UserModel> searchForUserStream(RealmModel realm, Map<String, String> params, Integer firstResult, Integer maxResults) {
        List<UserModel> users = new ArrayList<>();
        try {
            StringBuilder queryBuilder = new StringBuilder("SELECT c.Header, c.Item FROM c WHERE c.Item.Active = 1");
            List<SqlParameter> parameters = new ArrayList<>();
            String search = params.get("search");
            if (search != null && !search.isEmpty()) {
                queryBuilder.append(" AND (CONTAINS(LOWER(c.Header.UserAdId), @search) OR CONTAINS(LOWER(c.Item.Email), @search))");
                parameters.add(new SqlParameter("@search", search.toLowerCase()));
            }
            if (firstResult != null && maxResults != null) {
                queryBuilder.append(" OFFSET @offset LIMIT @limit");
                parameters.add(new SqlParameter("@offset", firstResult));
                parameters.add(new SqlParameter("@limit", maxResults));
            }
            SqlQuerySpec querySpec = new SqlQuerySpec(queryBuilder.toString(), parameters);
            CosmosPagedIterable<JsonNode> results = usersContainer.queryItems(querySpec, new CosmosQueryRequestOptions(), JsonNode.class);
            for (JsonNode userDoc : results) {
                if (isUserActive(userDoc)) {
                    String username = userDoc.has("Header") && userDoc.get("Header").has("UserAdId") ? userDoc.get("Header").get("UserAdId").asText() : null;
                    cacheAndReturn(username, userDoc);
                    users.add(new CosmosDbUserAdapter(session, realm, model, userDoc));
                }
            }
        } catch (Exception e) {
            logger.error("Error searching users", e);
        }
        return users.stream();
    }

    @Override
    public Stream<UserModel> getGroupMembersStream(RealmModel realm, GroupModel group, Integer firstResult, Integer maxResults) {
        return Stream.empty();
    }

    @Override
    public Stream<UserModel> searchForUserByUserAttributeStream(RealmModel realm, String attrName, String attrValue) {
        List<UserModel> users = new ArrayList<>();
        try {
            String query = "SELECT c.Header, c.Item FROM c WHERE c.Item." + attrName + " = @attrValue AND c.Item.Active = 1";
            SqlQuerySpec querySpec = new SqlQuerySpec(query, Collections.singletonList(new SqlParameter("@attrValue", attrValue)));
            CosmosPagedIterable<JsonNode> results = usersContainer.queryItems(querySpec, new CosmosQueryRequestOptions(), JsonNode.class);
            for (JsonNode userDoc : results) {
                if (isUserActive(userDoc)) {
                    String username = userDoc.has("Header") && userDoc.get("Header").has("UserAdId") ? userDoc.get("Header").get("UserAdId").asText() : null;
                    cacheAndReturn(username, userDoc);
                    users.add(new CosmosDbUserAdapter(session, realm, model, userDoc));
                }
            }
        } catch (Exception e) {
            logger.error("Error searching by attribute: " + attrName + " = " + attrValue, e);
        }

        return users.stream();
    }

    @Override
    public boolean updateCredential(RealmModel realm, UserModel user, CredentialInput input) {
        if (!supportsCredentialType(input.getType())) {
            logger.debugf("Unsupported credential type for update: %s", input.getType());
            return false;
        }
        String newPassword = input.getChallengeResponse();
        if (newPassword == null || newPassword.isBlank()) {
            logger.debug("New password is null or blank");
            return false;
        }
        try {
            JsonNode userDoc = findActiveUserByUsername(user.getUsername());
            if (userDoc == null) {
                logger.debugf("User document not found for password update: %s", user.getUsername());
                return false;
            }

            if (!userDoc.has("id")) {
                String query = "SELECT * FROM c WHERE c.Header.UserAdId = @uname";
                SqlQuerySpec querySpec = new SqlQuerySpec(query, Collections.singletonList(new SqlParameter("@uname", user.getUsername())));
                CosmosPagedIterable<JsonNode> results = usersContainer.queryItems(querySpec, new CosmosQueryRequestOptions(), JsonNode.class);
                for (JsonNode fullDoc : results) {
                    if (fullDoc.has("id")) {
                        userDoc = fullDoc;
                        break;
                    }
                }
            }
            if (!userDoc.has("id")) {
                logger.error("Cannot update password: document missing 'id' field for user " + user.getUsername());
                return false;
            }
            JsonNode item = userDoc.get("Item");
            if (item == null) {
                logger.debugf("Item section missing for user %s", user.getUsername());
                return false;
            }
            ((com.fasterxml.jackson.databind.node.ObjectNode) item).put("Password", newPassword);
            usersContainer.upsertItem(userDoc);
            logger.infof("Password updated in Cosmos DB for user %s", user.getUsername());
            userDocCache.put(user.getUsername(), userDoc);
            return true;
        } catch (Exception e) {
            logger.error("Error updating password in Cosmos DB for user " + user.getUsername(), e);
            return false;
        }
    }

    public void disableCredentialType(RealmModel realm, UserModel user, String credentialType) {
        // Pro Cosmos DB není podpora deaktivace typu hesla, metoda neprovádí žádnou akci ale musi zde být.
    }

    @Override
    public Stream<String> getDisableableCredentialTypesStream(RealmModel realm, UserModel user) {
        // Pro Cosmos DB není žádný typ hesla, který lze deaktivovat. Ale metoda musí existovat.
        return Stream.empty();
    }
}
