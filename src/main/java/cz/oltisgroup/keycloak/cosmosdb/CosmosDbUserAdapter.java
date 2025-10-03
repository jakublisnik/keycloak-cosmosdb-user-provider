package cz.oltisgroup.keycloak.cosmosdb;

import com.fasterxml.jackson.databind.JsonNode;
import org.jboss.logging.Logger;
import org.keycloak.component.ComponentModel;
import org.keycloak.credential.UserCredentialManager;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.models.SubjectCredentialManager;
import org.keycloak.storage.StorageId;
import org.keycloak.storage.adapter.AbstractUserAdapterFederatedStorage;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CosmosDbUserAdapter extends AbstractUserAdapterFederatedStorage {

    private static final Logger logger = Logger.getLogger(CosmosDbUserAdapter.class);

    private final JsonNode userDocument;
    private final JsonNode headerDoc;
    private final JsonNode itemDoc;
    private final String username;
    private final String email;
    private final boolean emailFromSource;
    private final boolean firstNameFromSource;
    private final boolean lastNameFromSource;

    public CosmosDbUserAdapter(KeycloakSession session, RealmModel realm,
                               ComponentModel model, JsonNode userDocument) {
        super(session, realm, model);
        this.userDocument = userDocument;
        this.headerDoc = userDocument.get("Header");
        this.itemDoc = userDocument.get("Item");

        this.username = headerDoc != null && headerDoc.has("UserAdId") ? headerDoc.get("UserAdId").asText() : null;
        String emailTmp = itemDoc != null && itemDoc.has("Email") && !itemDoc.get("Email").asText().isBlank() ? itemDoc.get("Email").asText() : null;
        this.email = emailTmp;


        this.firstNameFromSource = (itemDoc != null && (itemDoc.has("name") || itemDoc.has("Name")) &&
                !(firstNonBlank(itemDoc, "name", "Name") == null));
        this.lastNameFromSource = (itemDoc != null && (itemDoc.has("Surename") || itemDoc.has("Surname")) &&
                !(firstNonBlank(itemDoc, "Surename", "Surname") == null));
        this.emailFromSource = (this.email != null);

        logger.debugf("Created CosmosDbUserAdapter for user: %s (emailFromSource=%s, firstNameFromSource=%s, lastNameFromSource=%s)",
                username, emailFromSource, firstNameFromSource, lastNameFromSource);
    }

    private String firstNonBlank(JsonNode node, String... candidates) {
        if (node == null) return null;
        for (String c : candidates) {
            if (node.has(c) && !node.get(c).asText().isBlank()) {
                return node.get(c).asText();
            }
        }
        return null;
    }

    @Override
    public String getUsername() { return username; }
    @Override
    public void setUsername(String username) { /* read-only */ }

    @Override
    public String getEmail() {
        if (email != null && !email.isBlank()) {
            return email;
        }
        String stored = super.getFirstAttribute("email");
        return (stored == null || stored.isBlank()) ? null : stored;
    }

    @Override
    public void setEmail(String email) {
        if (emailFromSource) {
            logger.debugf("Ignoring setEmail for user %s – email pochází ze zdroje", username);
            return;
        }
        if (email == null || email.isBlank()) {
            logger.debugf("Clearing federated email for user %s", username);
            setSingleAttribute("email", null);
        } else {
            logger.debugf("Setting federated email for user %s -> %s", username, email);
            setSingleAttribute("email", email);
        }
    }

    @Override
    public boolean isEmailVerified() {
        String e = getEmail();
        if (e == null) return false;
        if (emailFromSource) return true;
        String flag = super.getFirstAttribute("emailVerified");
        return Boolean.parseBoolean(flag);
    }

    @Override
    public void setEmailVerified(boolean verified) {
        if (!emailFromSource) {
            setSingleAttribute("emailVerified", Boolean.toString(verified));
        }
    }

    @Override
    public String getFirstName() {
        String src = firstNonBlank(itemDoc, "name", "Name");
        if (src != null) return src;
        String stored = super.getFirstAttribute("firstName");
        return (stored == null || stored.isBlank()) ? null : stored;
    }

    @Override
    public void setFirstName(String firstName) {
        if (firstNameFromSource) {
            logger.debugf("Ignoring setFirstName for user %s – pochází ze zdroje", username);
            return;
        }
        if (firstName == null || firstName.isBlank()) {
            setSingleAttribute("firstName", null);
        } else {
            setSingleAttribute("firstName", firstName);
        }
    }

    @Override
    public String getLastName() {
        String src = firstNonBlank(itemDoc, "Surename", "Surname");
        if (src != null) return src;
        String stored = super.getFirstAttribute("lastName");
        return (stored == null || stored.isBlank()) ? null : stored;
    }

    @Override
    public void setLastName(String lastName) {
        if (lastNameFromSource) {
            logger.debugf("Ignoring setLastName for user %s – pochází ze zdroje", username);
            return;
        }
        if (lastName == null || lastName.isBlank()) {
            setSingleAttribute("lastName", null);
        } else {
            setSingleAttribute("lastName", lastName);
        }
    }

    @Override
    public boolean isEnabled() {
        return itemDoc != null && itemDoc.has("Active") && itemDoc.get("Active").asInt() == 1;
    }
    @Override
    public void setEnabled(boolean enabled) { /* read-only */ }

    @Override
    public String getId() { return StorageId.keycloakId(storageProviderModel, username); }

    // ---- Attribute exposure for User Profile ----
    @Override
    public Stream<String> getAttributeStream(String name) {
        switch (name) {
            case "firstName":
                String fn = getFirstName();
                return fn == null ? Stream.empty() : Stream.of(fn);
            case "lastName":
                String ln = getLastName();
                return ln == null ? Stream.empty() : Stream.of(ln);
            case "email":
                String em = getEmail();
                return em == null ? Stream.empty() : Stream.of(em);
            case "username":
                return username == null ? Stream.empty() : Stream.of(username);
            case "companyId":
                String tou = getCompanyId();
                return tou == null ? Stream.empty() : Stream.of(tou);
            case "userLWPId":
                String dou = getUserLWPId();
                return dou == null ? Stream.empty() : Stream.of(dou);
            default:
                // fallback původní logika
                if (headerDoc != null && headerDoc.has(name)) {
                    return Stream.of(headerDoc.get(name).asText());
                }
                if (itemDoc != null && itemDoc.has(name)) {
                    var node = itemDoc.get(name);
                    if (node.isArray()) {
                        return StreamSupport.stream(node.spliterator(), false).map(JsonNode::asText);
                    } else {
                        return Stream.of(node.asText());
                    }
                }
                List<String> stored = super.getAttributes().get(name);
                return stored == null ? Stream.empty() : stored.stream();
        }
    }

    @Override
    public String getFirstAttribute(String name) {
        return getAttributeStream(name).findFirst().orElse(null);
    }

    @Override
    public Map<String, List<String>> getAttributes() {
        Map<String, List<String>> m = new HashMap<>();
        if (username != null) m.put("username", List.of(username));
        String fn = getFirstName(); if (fn != null) m.put("firstName", List.of(fn));
        String ln = getLastName(); if (ln != null) m.put("lastName", List.of(ln));
        String em = getEmail(); if (em != null) m.put("email", List.of(em));
        String tou = getCompanyId(); if (tou != null) m.put("companyId", List.of(tou));
        String dou = getUserLWPId(); if (dou != null) m.put("userLWPId", List.of(dou));
        if (itemDoc != null && itemDoc.has("Active")) {
            m.put("Active", List.of(itemDoc.get("Active").asText()));
        }
        super.getAttributes().forEach((k,v) -> m.putIfAbsent(k, v));
        return Collections.unmodifiableMap(m);
    }

    @Override
    public SubjectCredentialManager credentialManager() {
        return new UserCredentialManager(session, realm, this);
    }

    @Override
    public Long getCreatedTimestamp() { return null; }
    @Override
    public void setCreatedTimestamp(Long timestamp) { /* read-only */ }

    public String getCompanyId() {
        if (headerDoc != null && headerDoc.has("CompanyId")) {
            return headerDoc.get("CompanyId").asText();
        }
        String stored = super.getFirstAttribute("companyId");
        return (stored == null || stored.isBlank()) ? null : stored;
    }

    public String getUserLWPId() {
        if (headerDoc != null && headerDoc.has("UserLWPId")) {
            return headerDoc.get("UserLWPId").asText();
        }
        String stored = super.getFirstAttribute("userLWPId");
        return (stored == null || stored.isBlank()) ? null : stored;
    }

}
