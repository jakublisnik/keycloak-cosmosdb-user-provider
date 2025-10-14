package cz.oltisgroup.keycloak.cosmosdb;

import com.azure.cosmos.*;
import com.azure.cosmos.models.*;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jboss.logging.Logger;
import org.keycloak.component.ComponentModel;
import org.keycloak.credential.CredentialInput;
import org.keycloak.credential.CredentialInputUpdater;
import org.keycloak.credential.CredentialInputValidator;
import org.keycloak.http.HttpRequest;
import org.keycloak.models.*;
import org.keycloak.models.credential.PasswordCredentialModel;
import org.keycloak.storage.StorageId;
import org.keycloak.storage.UserStorageProvider;
import org.keycloak.storage.user.UserLookupProvider;
import org.keycloak.storage.user.UserQueryProvider;
import org.keycloak.storage.user.UserRegistrationProvider;

import java.util.*;
import java.util.stream.Stream;

public class CosmosDbUserStorageProvider implements UserStorageProvider,
        UserLookupProvider, CredentialInputValidator, UserQueryProvider, CredentialInputUpdater, UserRegistrationProvider {

    private static final Logger logger = Logger.getLogger(CosmosDbUserStorageProvider.class);

    private final KeycloakSession session;
    private final ComponentModel model;
    private final CosmosClient cosmosClient;
    private final CosmosContainer usersContainer;
    private final CosmosContainer usersExtraContainer;

    private final String endpoint;
    private final String key;
    private final String databaseName;
    private final String containerName;
    private final int keepAliveSeconds;

    private final Map<String, JsonNode> userDocCache = new HashMap<>();
    private final CosmosDbExtraUserOps extraOps;

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
        String usersContainerName = model.get(CosmosDbUserStorageProviderFactory.USERS_CONTAINER_NAME, "Users");
        this.usersExtraContainer = database.getContainer(usersContainerName);
        this.extraOps = new CosmosDbExtraUserOps(usersExtraContainer, logger);

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
            throw new ModelException("Error querying user by username", e);
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
            return new CosmosDbUserAdapter(session, realm, model, userDoc, this);
        }
        logger.debugf("No active user found for %s", username);
        return null;
    }

    @Override
    public UserModel getUserByEmail(RealmModel realm, String email) {
        logger.debugf("getUserByEmail called with email: %s", email);
        try {
            String query = "SELECT c.Header, c.Item FROM c WHERE c.Item.Email = @email OR c.Item.email = @email";
            SqlQuerySpec querySpec = new SqlQuerySpec(query, Collections.singletonList(new SqlParameter("@email", email)));
            CosmosPagedIterable<JsonNode> results = usersContainer.queryItems(querySpec, new CosmosQueryRequestOptions(), JsonNode.class);
            for (JsonNode userDoc : results) {
                if (isUserActive(userDoc)) {
                    String username = userDoc.has("Header") && userDoc.get("Header").has("UserAdId") ? userDoc.get("Header").get("UserAdId").asText() : null;
                    cacheAndReturn(username, userDoc);
                    return new CosmosDbUserAdapter(session, realm, model, userDoc, this);
                }
            }
        } catch (Exception e) {
            logger.error("Error fetching user by email: " + email, e);
            throw new ModelException("Error fetching user by email", e);
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
            String query = "SELECT VALUE COUNT(1) FROM c";
            CosmosPagedIterable<Integer> results = usersContainer.queryItems(query, new CosmosQueryRequestOptions(), Integer.class);
            for (Integer count : results) {
                return count;
            }
        } catch (Exception e) {
            logger.error("Error getting users count", e);
            throw new ModelException("Error getting users count", e);
        }
        return 0;
    }

    @Override
    public Stream<UserModel> searchForUserStream(RealmModel realm, Map<String, String> params, Integer firstResult, Integer maxResults) {
        logger.info("SEARCH FOR USER STREAM CALLED, PARAMS: " + params);
        List<UserModel> users = new ArrayList<>();
        try {
            StringBuilder queryBuilder = new StringBuilder("SELECT c.Header, c.Item FROM c");
            List<SqlParameter> parameters = new ArrayList<>();
            String search = null;
            for (Map.Entry<String, String> entry : params.entrySet()) {
                if (entry.getKey().toLowerCase().contains("search")) {
                    search = entry.getValue();
                    break;
                }
            }
            logger.info("SEARCH paramko: " + search);
            // Only add WHERE if search is not "*" and not empty
            if (search != null && !search.isEmpty() && !search.equals("*")) {
                queryBuilder.append(" WHERE (CONTAINS(LOWER(c.Header.UserAdId), @search) OR CONTAINS(LOWER(c.Item.Email), @search) OR CONTAINS(LOWER(c.Item.email), @search))");
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
                String username = userDoc.has("Header") && userDoc.get("Header").has("UserAdId") ? userDoc.get("Header").get("UserAdId").asText() : null;
                cacheAndReturn(username, userDoc);
                users.add(new CosmosDbUserAdapter(session, realm, model, userDoc, this));
            }
        } catch (Exception e) {
            logger.error("Error searching users", e);
            throw new ModelException("Error searching users", e);
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
                    users.add(new CosmosDbUserAdapter(session, realm, model, userDoc, this));
                }
            }
        } catch (Exception e) {
            logger.error("Error searching by attribute: " + attrName + " = " + attrValue, e);
            throw new ModelException("Error searching users by attribute", e);
        }

        return users.stream();
    }

    @Override
    public boolean updateCredential(RealmModel realm, UserModel user, CredentialInput input) {
        if (!supportsCredentialType(input.getType())) {
            logger.debugf("Unsupported credential type for update: %s", input.getType());
            throw new ModelException("Unsupported credential type: " + input.getType());
        }
        String newPassword = input.getChallengeResponse();
        if (newPassword == null || newPassword.isBlank()) {
            logger.debug("New password is null or blank");
            throw new ModelException("New password cannot be null or blank");
        }
        try {
            JsonNode userDoc = findActiveUserByUsername(user.getUsername());
            if (userDoc == null) {
                logger.debugf("User document not found for password update: %s", user.getUsername());
                throw new ModelException("User document not found for password update");
            }

            if (!userDoc.has("id")) {
                String query = "SELECT * FROM c WHERE LOWER(c.Header.UserAdId) = @uname";
                SqlQuerySpec querySpec = new SqlQuerySpec(query, Collections.singletonList(new SqlParameter("@uname", user.getUsername().toLowerCase(Locale.ROOT))));
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
                throw new ModelException("User document missing 'id' field");
            }
            JsonNode item = userDoc.get("Item");
            if (item == null) {
                logger.debugf("Item section missing for user %s", user.getUsername());
                throw new ModelException("User document missing 'Item' section");
            }
            ((com.fasterxml.jackson.databind.node.ObjectNode) item).put("Password", newPassword);
            usersContainer.upsertItem(userDoc);
            logger.infof("Password updated in Cosmos DB for user %s", user.getUsername());
            userDocCache.put(user.getUsername(), userDoc);
            userDocCache.put(user.getUsername().toLowerCase(Locale.ROOT), userDoc);
            extraOps.updateCredential(user.getUsername(), newPassword);
            return true;
        } catch (Exception e) {
            logger.error("Error updating password in Cosmos DB for user " + user.getUsername(), e);
            throw new ModelException("Error updating password in Cosmos DB for user");
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

    @Override
    public UserModel addUser(RealmModel realm, String username) {
        try {
            String documentId = java.util.UUID.randomUUID().toString();

            logger.infof("=== DEBUG: addUser called for: %s ===", username);

            Map<String, Object> header = new HashMap<>();
            header.put("UserAdId", username);

            Map<String, Object> item = new HashMap<>();
            item.put("Active", 1);

            // Capture email from form if present (only Item.Email)
            try {
                KeycloakContext ctx = session.getContext();
                HttpRequest req = ctx != null ? ctx.getHttpRequest() : null;
                if (req != null) {
                    jakarta.ws.rs.core.MultivaluedMap<String, String> form = req.getDecodedFormParameters();
                    String email = form != null ? form.getFirst("email") : null;
                    if (email != null && !email.isBlank()) {
                        item.put("Email", email);
                    }
                }
            } catch (Exception ignored) {
                throw new ModelException("Error capturing email from form during user creation");
            }

            Map<String, Object> userDoc = new java.util.LinkedHashMap<>();
            userDoc.put("Header", header);
            userDoc.put("Item", item);
            userDoc.put("id", documentId);

            header.put("Guid", java.util.UUID.randomUUID().toString());
            header.put("SrcSystem", "AZURE");
            header.put("DstSystem", "mda");
            header.put("IntegrationID", "220");
            header.put("IntegrationVer", "1.01");
            header.put("IntegrationType", "Request");
            header.put("Description", "Users");
            header.put("SrcVersion", null);
            header.put("ModeShift", 0);
            header.put("ProfId", null);
            header.put("Language", null);
            header.put("DeviceId", null);
            header.put("GPSLongtitude", null);
            header.put("GPSLatitude", null);
            String now = java.time.OffsetDateTime.now().toString();
            header.put("TimeStamp", now);
            header.put("TimeCreation", now);
            header.put("TimeSend", now);

            String passwordExp = java.time.OffsetDateTime.now().plusYears(1).toString();
            item.put("PasswordExpiration", passwordExp);
            item.put("PasswordChange", 0);


            usersContainer.createItem(userDoc);
            logger.infof("User %s created (minimal doc)", username);

            JsonNode asJson = new ObjectMapper().convertValue(userDoc, JsonNode.class);
            return new CosmosDbUserAdapter(session, realm, model, asJson, this);

        } catch (Exception e) {
            logger.error("Failed to create user: " + username, e);
            throw new ModelException("Error creating user in Cosmos DB: " + username, e);
        }
    }

    // --- Helpers for persisting profile changes from Keycloak ---
    public void updateUserNames(String username, String firstNameOrNull, String lastNameOrNull) {
        if ((firstNameOrNull == null || firstNameOrNull.isBlank()) && (lastNameOrNull == null || lastNameOrNull.isBlank())) {
            return; // nothing to do
        }
        try {
            JsonNode userDoc = findActiveUserByUsername(username);
            if (userDoc == null) {
                logger.debugf("updateUserNames: user not found: %s", username);
                return;
            }
            // ensure full doc with id
            if (!userDoc.has("id")) {
                String q = "SELECT * FROM c WHERE LOWER(c.Header.UserAdId) = @uname";
                SqlQuerySpec spec = new SqlQuerySpec(q, Collections.singletonList(new SqlParameter("@uname", username.toLowerCase(Locale.ROOT))));
                CosmosPagedIterable<JsonNode> res = usersContainer.queryItems(spec, new CosmosQueryRequestOptions(), JsonNode.class);
                for (JsonNode full : res) { userDoc = full; break; }
            }
            if (!userDoc.has("id")) {
                logger.warnf("updateUserNames: missing id for user %s", username);
                return;
            }
            JsonNode item = userDoc.get("Item");
            if (item == null || !item.isObject()) {
                logger.warnf("updateUserNames: missing Item for user %s", username);
                return;
            }
            com.fasterxml.jackson.databind.node.ObjectNode itemObj = (com.fasterxml.jackson.databind.node.ObjectNode) item;
            if (firstNameOrNull != null && !firstNameOrNull.isBlank()) {
                itemObj.put("Name", firstNameOrNull);
            }
            if (lastNameOrNull != null && !lastNameOrNull.isBlank()) {
                itemObj.put("Surename", lastNameOrNull);
            }
            usersContainer.upsertItem(userDoc);
            // refresh cache
            userDocCache.put(username, userDoc);
            userDocCache.put(username.toLowerCase(Locale.ROOT), userDoc);
            logger.debugf("updateUserNames: persisted for %s (firstName set=%s, lastName set=%s)", username,
                    firstNameOrNull != null, lastNameOrNull != null);
            // Update extra collection as well
            extraOps.updateUserNames(username, firstNameOrNull, lastNameOrNull);
        } catch (Exception ex) {
            logger.error("updateUserNames failed for user " + username, ex);
            throw new RuntimeException("Error updating names for user " + username, ex);
        }
    }


    public void updateEmail(String username, String email) {
        try {
            JsonNode userDoc = findActiveUserByUsername(username);
            if (userDoc == null) {
                // Fallback for inactive users: load full doc by username
                String q = "SELECT * FROM c WHERE LOWER(c.Header.UserAdId) = @uname";
                SqlQuerySpec spec = new SqlQuerySpec(q, Collections.singletonList(new SqlParameter("@uname", username.toLowerCase(Locale.ROOT))));
                CosmosPagedIterable<JsonNode> res = usersContainer.queryItems(spec, new CosmosQueryRequestOptions(), JsonNode.class);
                for (JsonNode full : res) { userDoc = full; break; }
                if (userDoc == null) {
                    logger.infof("updateEmail: user not found (even fallback): %s", username);
                    return;
                }
            }
            if (!userDoc.has("id")) {
                String q = "SELECT * FROM c WHERE LOWER(c.Header.UserAdId) = @uname";
                SqlQuerySpec spec = new SqlQuerySpec(q, Collections.singletonList(new SqlParameter("@uname", username.toLowerCase(Locale.ROOT))));
                CosmosPagedIterable<JsonNode> res = usersContainer.queryItems(spec, new CosmosQueryRequestOptions(), JsonNode.class);
                for (JsonNode full : res) { userDoc = full; break; }
            }
            if (!userDoc.has("id")) {
                logger.infof("updateEmail: missing id for user %s", username);
                return;
            }
            JsonNode item = userDoc.get("Item");
            if (item == null || !item.isObject()) {
                logger.infof("updateEmail: missing Item for user %s", username);
                return;
            }
            com.fasterxml.jackson.databind.node.ObjectNode itemObj = (com.fasterxml.jackson.databind.node.ObjectNode) item;
            if (email == null || email.isBlank()) {
                itemObj.remove("Email");
                itemObj.remove("email");
            } else {
                itemObj.put("Email", email);
                itemObj.remove("email"); // remove lowercase variant to enforce single key
            }
            usersContainer.upsertItem(userDoc);
            userDocCache.put(username, userDoc);
            userDocCache.put(username.toLowerCase(Locale.ROOT), userDoc);
            logger.infof("updateEmail: persisted for %s -> %s", username, email);
            // Update extra collection as well
            extraOps.updateEmail(username, email);
        } catch (Exception ex) {
            logger.error("updateEmail failed for user " + username, ex);
            throw new RuntimeException("Error updating email for user " + username, ex);
        }
    }

    public void updateActive(String username, boolean enabled) {
        try {
            JsonNode userDoc = findActiveUserByUsername(username);
            if (userDoc == null) {
                // If user is not returned by findActiveUserByUsername because currently inactive, try by exact username
                String q = "SELECT * FROM c WHERE LOWER(c.Header.UserAdId) = @uname";
                SqlQuerySpec spec = new SqlQuerySpec(q, Collections.singletonList(new SqlParameter("@uname", username.toLowerCase(Locale.ROOT))));
                CosmosPagedIterable<JsonNode> res = usersContainer.queryItems(spec, new CosmosQueryRequestOptions(), JsonNode.class);
                for (JsonNode full : res) { userDoc = full; break; }
            }
            if (userDoc == null || !userDoc.has("id")) {
                logger.warnf("updateActive: user doc missing or no id for %s", username);
                return;
            }
            JsonNode item = userDoc.get("Item");
            if (item == null || !item.isObject()) {
                logger.warnf("updateActive: missing Item for user %s", username);
                return;
            }
            com.fasterxml.jackson.databind.node.ObjectNode itemObj = (com.fasterxml.jackson.databind.node.ObjectNode) item;
            itemObj.put("Active", enabled ? 1 : 0);
            usersContainer.upsertItem(userDoc);
            userDocCache.put(username, userDoc);
            userDocCache.put(username.toLowerCase(Locale.ROOT), userDoc);
            logger.debugf("updateActive: persisted for %s -> %s", username, enabled);
        } catch (Exception ex) {
            logger.error("updateActive failed for user " + username, ex);
            throw new ModelException("Error updating active status in Cosmos DB for user " + username, ex);
        }
    }

    // Update Header attributes like CompanyId and UserLWPId
    public void updateHeaderAttributes(String username, String companyIdOrNull, String userLWPIdOrNull) {
        logger.info("COMPANY ID FIRMA ZACATEK: " + companyIdOrNull);
        if ((companyIdOrNull == null || companyIdOrNull.isBlank()) && (userLWPIdOrNull == null || userLWPIdOrNull.isBlank())) {
            return;
        }
        try {
            JsonNode userDoc = findActiveUserByUsername(username);
            if (userDoc == null) {
                logger.debugf("updateHeaderAttributes: user not found: %s", username);
                return;
            }
            if (!userDoc.has("id")) {
                String q = "SELECT * FROM c WHERE LOWER(c.Header.UserAdId) = @uname";
                SqlQuerySpec spec = new SqlQuerySpec(q, Collections.singletonList(new SqlParameter("@uname", username.toLowerCase(Locale.ROOT))));
                CosmosPagedIterable<JsonNode> res = usersContainer.queryItems(spec, new CosmosQueryRequestOptions(), JsonNode.class);
                for (JsonNode full : res) { userDoc = full; break; }
            }
            if (!userDoc.has("id")) {
                logger.warnf("updateHeaderAttributes: missing id for user %s", username);
                return;
            }
            JsonNode header = userDoc.get("Header");
            if (header == null || !header.isObject()) {
                logger.warnf("updateHeaderAttributes: missing Header for user %s", username);
                return;
            }
            com.fasterxml.jackson.databind.node.ObjectNode headerObj = (com.fasterxml.jackson.databind.node.ObjectNode) header;
            if (companyIdOrNull != null && !companyIdOrNull.isBlank()) {
                headerObj.put("CompanyId", companyIdOrNull);
            }
            if (userLWPIdOrNull != null && !userLWPIdOrNull.isBlank()) {
                headerObj.put("UserLWPId", userLWPIdOrNull);
            }
            usersContainer.upsertItem(userDoc);
            userDocCache.put(username, userDoc);
            userDocCache.put(username.toLowerCase(Locale.ROOT), userDoc);
            logger.debugf("updateHeaderAttributes: persisted for %s (CompanyId set=%s, UserLWPId set=%s)", username,
                    companyIdOrNull != null, userLWPIdOrNull != null);
            // Update extra collection as well
            extraOps.updateHeaderAttributes(username, companyIdOrNull, userLWPIdOrNull);
        } catch (Exception ex) {
            logger.error("updateHeaderAttributes failed for user " + username, ex);
            throw new RuntimeException("Error updating header attributes for user " + username, ex);
        }

    }

    @Override
    public boolean removeUser(RealmModel realm, UserModel user) {
        String username = user.getUsername();
        try {
            // Find user document in main collection
            JsonNode userDoc = findActiveUserByUsername(username);
            if (userDoc == null || !userDoc.has("id")) {
                // Try to load full doc by username if not found
                String q = "SELECT * FROM c WHERE LOWER(c.Header.UserAdId) = @uname";
                SqlQuerySpec spec = new SqlQuerySpec(q, Collections.singletonList(new SqlParameter("@uname", username.toLowerCase(Locale.ROOT))));
                CosmosPagedIterable<JsonNode> res = usersContainer.queryItems(spec, new CosmosQueryRequestOptions(), JsonNode.class);
                for (JsonNode full : res) { userDoc = full; break; }
            }
            if (userDoc != null && userDoc.has("id")) {
                String id = userDoc.get("id").asText();
                String partitionKeyValue = userDoc.get("Header").get("UserAdId").asText();
                usersContainer.deleteItem(id, new PartitionKey(partitionKeyValue), new CosmosItemRequestOptions());
                userDocCache.remove(username);
                userDocCache.remove(username.toLowerCase(Locale.ROOT));
            }

            // Remove from extra collection
            extraOps.removeUser(username);


            logger.infof("User %s deleted from both Cosmos DB collections and Keycloak", username);
            return true;
        } catch (Exception ex) {
            logger.error("Failed to remove user " + username, ex);
            return false;
        }
    }


    public CosmosDbExtraUserOps getExtraOps() {
        return extraOps;
    }

}
