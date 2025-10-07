package cz.oltisgroup.keycloak.cosmosdb;

import com.azure.cosmos.*;
import com.azure.cosmos.models.*;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.JsonNode;
import org.jboss.logging.Logger;

import java.util.Collections;

public class CosmosDbExtraUserOps {

    private final CosmosContainer usersExtraContainer;
    private final Logger logger;

    public CosmosDbExtraUserOps(CosmosContainer usersExtraContainer, Logger logger) {
        this.usersExtraContainer = usersExtraContainer;
        this.logger = logger;
    }

    public void updateEmail(String username, String email) {
        logger.info("UPDATE EMAIL V EXTRA KOLEKCI PRES PROVIDERA");
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
        logger.info("UPDATE ATTRIBUTES V EXTRA KOLEKCI PRES PROVIDERA");
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
                        logger.warnf("updateHeaderAttributesExtraCollection: lwpId '%s' není číslo, ignorováno.", userLWPIdOrNull);
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
}

