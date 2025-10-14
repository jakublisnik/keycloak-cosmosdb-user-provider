package cz.oltisgroup.keycloak.cosmosdb;

import com.azure.cosmos.*;
import com.azure.cosmos.models.*;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.JsonNode;
import org.jboss.logging.Logger;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class CosmosDbExtraUserOps {

    private final CosmosContainer usersExtraContainer;
    private final Logger logger;

    public CosmosDbExtraUserOps(CosmosContainer usersExtraContainer, Logger logger) {
        this.usersExtraContainer = usersExtraContainer;
        this.logger = logger;
    }

    public void updateEmail(String username, String email) {
        try {
            String q = "SELECT * FROM c WHERE c.login = @login";
            SqlQuerySpec spec = new SqlQuerySpec(q, Collections.singletonList(new SqlParameter("@login", username)));
            CosmosPagedIterable<JsonNode> res = usersExtraContainer.queryItems(spec, new CosmosQueryRequestOptions(), JsonNode.class);
            for (JsonNode doc : res) {
                if (!doc.isObject()) continue;
                com.fasterxml.jackson.databind.node.ObjectNode obj = (com.fasterxml.jackson.databind.node.ObjectNode) doc;
                if (email == null || email.isBlank()) {
                    obj.put("email", "");
                } else {
                    obj.put("email", email);
                }
                usersExtraContainer.upsertItem(obj);
                logger.infof("updateEmailExtraCollection: persisted for %s -> %s", username, email);
                break;
            }
        } catch (Exception ex) {
            logger.error("updateEmailExtraCollection failed for user " + username, ex);
            throw new RuntimeException("Error updating email in extra Users collection for user " + username, ex);
        }
    }

    public void updateHeaderAttributes(String username, String companyIdOrNull, String userLWPIdOrNull) {
        if ((companyIdOrNull == null || companyIdOrNull.isBlank()) && (userLWPIdOrNull == null || userLWPIdOrNull.isBlank())) {
            return;
        }
        try {
            String q = "SELECT * FROM c WHERE c.login = @login";
            SqlQuerySpec spec = new SqlQuerySpec(q, Collections.singletonList(new SqlParameter("@login", username)));
            CosmosPagedIterable<JsonNode> res = usersExtraContainer.queryItems(spec, new CosmosQueryRequestOptions(), JsonNode.class);
            for (JsonNode doc : res) {
                if (!doc.isObject()) continue;
                com.fasterxml.jackson.databind.node.ObjectNode obj = (com.fasterxml.jackson.databind.node.ObjectNode) doc;
                if (companyIdOrNull != null && !companyIdOrNull.isBlank()) {
                    obj.put("firmaId", companyIdOrNull);
                }
                if (userLWPIdOrNull != null && !userLWPIdOrNull.isBlank()) {
                    try {
                        int lwpIdInt = Integer.parseInt(userLWPIdOrNull);
                        obj.put("lwpId", lwpIdInt);
                    } catch (NumberFormatException e) {
                        logger.warnf("updateHeaderAttributesExtraCollection: lwpId '%s' , .", userLWPIdOrNull);
                    }
                }
                usersExtraContainer.upsertItem(obj);
                logger.infof("updateHeaderAttributesExtraCollection: persisted for %s (firmaId set=%s, lwpId set=%s)", username,
                        companyIdOrNull != null, userLWPIdOrNull != null);
                break;
            }
        } catch (Exception ex) {
            logger.error("updateHeaderAttributesExtraCollection failed for user " + username, ex);
            throw new RuntimeException("Error updating header attributes in extra Users collection for user " + username, ex);
        }
    }

    public void updateUserNames(String username, String firstNameOrNull, String lastNameOrNull) {
        if ((firstNameOrNull == null || firstNameOrNull.isBlank()) && (lastNameOrNull == null || lastNameOrNull.isBlank())) {
            return;
        }
        try {
            String q = "SELECT * FROM c WHERE c.login = @login";
            SqlQuerySpec spec = new SqlQuerySpec(q, Collections.singletonList(new SqlParameter("@login", username)));
            CosmosPagedIterable<JsonNode> res = usersExtraContainer.queryItems(spec, new CosmosQueryRequestOptions(), JsonNode.class);
            for (JsonNode doc : res) {
                if (!doc.isObject()) continue;
                com.fasterxml.jackson.databind.node.ObjectNode obj = (com.fasterxml.jackson.databind.node.ObjectNode) doc;
                if (firstNameOrNull != null && !firstNameOrNull.isBlank()) {
                    obj.put("name", firstNameOrNull);
                }
                if (lastNameOrNull != null && !lastNameOrNull.isBlank()) {
                    obj.put("surename", lastNameOrNull);
                }
                usersExtraContainer.upsertItem(obj);
                logger.infof("updateUserNamesExtraCollection: persisted for %s (name set=%s, surename set=%s)", username,
                        firstNameOrNull != null, lastNameOrNull != null);
                break;
            }
        } catch (Exception ex) {
            logger.error("updateUserNamesExtraCollection failed for user " + username, ex);
            throw new RuntimeException("Error updating names in extra Users collection for user " + username, ex);
        }
    }

    public void updateCredential(String username, String newPassword) {
        if (newPassword == null || newPassword.isBlank()) {
            logger.debug("updateCredentialExtraCollection: New password is null or blank");
            throw new RuntimeException("New password cannot be null or blank");
        }
        try {
            String q = "SELECT * FROM c WHERE c.login = @login";
            SqlQuerySpec spec = new SqlQuerySpec(q, Collections.singletonList(new SqlParameter("@login", username)));
            CosmosPagedIterable<JsonNode> res = usersExtraContainer.queryItems(spec, new CosmosQueryRequestOptions(), JsonNode.class);
            for (JsonNode doc : res) {
                if (!doc.isObject()) continue;
                com.fasterxml.jackson.databind.node.ObjectNode obj = (com.fasterxml.jackson.databind.node.ObjectNode) doc;
                obj.put("password", newPassword);
                usersExtraContainer.upsertItem(obj);
                logger.infof("updateCredentialExtraCollection: password updated for %s", username);
                break;
            }
        } catch (Exception e) {
            logger.error("updateCredentialExtraCollection failed for user " + username, e);
            throw new RuntimeException("Error updating password in extra Users collection for user " + username, e);
        }
    }

    public void createUserInExtraCollection(String username) {
        logger.infof("Creating user %s in extra Users collection", username);
        String passwordExp = java.time.OffsetDateTime.now().plusYears(1).toString();
        try {
            Map<String, Object> usersDoc = new LinkedHashMap<>();
            usersDoc.put("login", username);
            usersDoc.put("name", "");
            usersDoc.put("surename", "");
            usersDoc.put("password", "");
            usersDoc.put("lwpId", "");
            usersDoc.put("firmaId", "");
            usersDoc.put("role", "user");
            usersDoc.put("active", 1);
            usersDoc.put("phone", "");
            usersDoc.put("email", "");
            usersDoc.put("passwordExpiration", passwordExp);
            usersDoc.put("passwordChange", 0);
            usersDoc.put("id", java.util.UUID.randomUUID().toString());
            usersExtraContainer.createItem(usersDoc);
            logger.infof("User %s successfully created in extra Users collection", username);
        } catch (Exception ex) {
            logger.errorf("Failed to create user %s in extra Users collection", username, ex);
            throw new RuntimeException("Error creating user in extra Users collection for user " + username, ex);
        }
    }

    public boolean existsInExtraCollection(String username) {
        try {
            String q = "SELECT * FROM c WHERE c.login = @login";
            SqlQuerySpec spec = new SqlQuerySpec(q, Collections.singletonList(new SqlParameter("@login", username)));
            CosmosPagedIterable<JsonNode> res = usersExtraContainer.queryItems(spec, new CosmosQueryRequestOptions(), JsonNode.class);
            for (JsonNode doc : res) {
                if (doc.isObject()) {
                    logger.debugf("existsInExtraCollection: user %s exists", username);
                    return true;
                }
            }
            logger.debugf("existsInExtraCollection: user %s does not exist", username);
            return false;
        } catch (Exception ex) {
            logger.error("existsInExtraCollection failed for user " + username, ex);
            throw new RuntimeException("Error checking existence in extra Users collection for user " + username, ex);
        }
    }

    public void removeUser(String username) {
        try {
            String q = "SELECT * FROM c WHERE c.login = @login";
            SqlQuerySpec spec = new SqlQuerySpec(q, Collections.singletonList(new SqlParameter("@login", username)));
            CosmosPagedIterable<JsonNode> res = usersExtraContainer.queryItems(spec, new CosmosQueryRequestOptions(), JsonNode.class);
            for (JsonNode doc : res) {
                if (doc.isObject() && doc.has("id")) {
                    String id = doc.get("id").asText();
                    usersExtraContainer.deleteItem(id, new PartitionKey(username), new CosmosItemRequestOptions());
                    logger.infof("removeUserExtraCollection: user %s deleted from extra collection", username);
                    break;
                }
            }
        } catch (Exception ex) {
            logger.error("removeUserExtraCollection failed for user " + username, ex);
            throw new RuntimeException("Error removing user from extra Users collection for user " + username, ex);
        }
    }

}
