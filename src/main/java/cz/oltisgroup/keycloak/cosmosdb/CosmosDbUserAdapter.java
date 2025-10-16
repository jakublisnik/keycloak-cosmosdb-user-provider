package cz.oltisgroup.keycloak.cosmosdb;

import com.fasterxml.jackson.databind.JsonNode;
import org.jboss.logging.Logger;
import org.keycloak.component.ComponentModel;
import org.keycloak.credential.UserCredentialManager;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.ModelException;
import org.keycloak.models.RealmModel;
import org.keycloak.models.SubjectCredentialManager;
import org.keycloak.storage.StorageId;
import org.keycloak.storage.adapter.AbstractUserAdapterFederatedStorage;

import java.time.OffsetDateTime;
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
    // Injected provider to avoid deprecated session.getProvider/component provider calls
    private final CosmosDbUserStorageProvider provider;

    public CosmosDbUserAdapter(KeycloakSession session, RealmModel realm,
                               ComponentModel model, JsonNode userDocument,
                               CosmosDbUserStorageProvider provider) {
        super(session, realm, model);
        this.userDocument = userDocument;
        this.headerDoc = userDocument.get("Header");
        this.itemDoc = userDocument.get("Item");
        this.provider = provider;

        this.username = headerDoc != null && headerDoc.has("UserAdId") ? headerDoc.get("UserAdId").asText() : null;
        this.email = itemDoc != null ? firstNonBlank(itemDoc, "Email", "email") : null;

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
        // Prefer federated attribute for immediate consistency after admin edits, fallback to source
        String stored = super.getFirstAttribute("email");
        if (stored != null && !stored.isBlank()) {
            return stored;
        }
        if (email != null && !email.isBlank()) {
            return email;
        }
        return null;
    }

    @Override
    public void setEmail(String email) {
        // Persist to Cosmos DB as source-of-truth as well
        try {
            if (provider != null) {
                provider.updateEmail(username, email);
            }
        } catch (Exception ex) {
            logger.debugf("Failed to persist email for user %s into Cosmos DB: %s", username, ex.getMessage());
            throw new ModelException("Failed to persist email into Cosmos DB.");
        }
        // Keep federated attribute for UI consistency when editing
        if (email == null || email.isBlank()) {
            setSingleAttribute("email", null);
        } else {
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
        // Persist to Cosmos DB
        try {
            if (provider != null) {
                provider.updateUserNames(username, firstName, null);
            }
        } catch (Exception ex) {
            logger.debugf("Failed to persist firstName for user %s into Cosmos DB: %s", username, ex.getMessage());
            throw new ModelException("Failed to persist firstName into Cosmos DB.");
        }
        // Keep federated attribute for immediate UI consistency
        setSingleAttribute("firstName", (firstName == null || firstName.isBlank()) ? null : firstName);
    }

    // Intercept UserProfile attribute updates as well
    @Override
    public void setSingleAttribute(String name, String value) {
        String normalized = name == null ? "" : name.trim().toLowerCase();

        try {
            switch (normalized) {
                case "firstname":
                case "lastname":
                    if (provider != null) {
                        String fn = "firstname".equals(normalized) ? value : null;
                        String ln = "lastname".equals(normalized) ? value : null;
                        provider.updateUserNames(username, fn, ln);
                    }
                    break;
                case "companyid":
                case "userlwpid":
                    if (provider != null) {
                        String cid = "companyid".equals(normalized) ? value : null;
                        String lid = "userlwpid".equals(normalized) ? value : null;
                        provider.updateHeaderAttributes(username, cid, lid);
                    }
                    break;
                case "email":
                    if (provider != null) {
                        provider.updateEmail(username, value);
                    }
                    break;
                default:
                    // No special handling
            }
        } catch (Exception ex) {
            logger.debugf("Failed to persist attribute %s for user %s: %s", name, username, ex.getMessage());
            throw new ModelException("Failed to persist " + name + " into Cosmos DB.");
        }

        super.setSingleAttribute(name, value);
    }


    @Override
    public void setAttribute(String name, List<String> values) {
        String normalized = name == null ? "" : name.trim().toLowerCase();
        String value = (values == null || values.isEmpty()) ? null : values.get(0);
        try {
            switch (normalized) {
                case "firstname":
                case "lastname":
                    if (provider != null) {
                        String fn = "firstname".equals(normalized) ? value : null;
                        String ln = "lastname".equals(normalized) ? value : null;
                        provider.updateUserNames(username, fn, ln);
                    }
                    break;
                case "companyid":
                case "userlwpid":
                    if (provider != null) {
                        String cid = "companyid".equals(normalized) ? value : null;
                        String lid = "userlwpid".equals(normalized) ? value : null;
                        provider.updateHeaderAttributes(username, cid, lid);
                    }
                    break;
                case "email":
                    if (provider != null) {
                        provider.updateEmail(username, value);
                    }
                    break;
                case "savetoseccondcollection":
                    if (provider != null && "yes".equalsIgnoreCase(value)) {
                        var extraOps = provider.getExtraOps();
                        if (!extraOps.existsInExtraCollection(username)) {
                            logger.infof("saveToSeccondCollection set to 'yes' for user %s, creating in extra collection.", username);
                            extraOps.createUserInExtraCollection(username);
                            extraOps.updateHeaderAttributes(username, getFirstAttribute("CompanyId"), getFirstAttribute("userLWPId"));
                            extraOps.updateEmail(username, getEmail());
                            extraOps.updateUserNames(username, getFirstName(), getLastName());
                        } else {
                            logger.infof("User %s already exists in extra collection, skipping creation.", username);
                        }
                    }
                    break;
                default:
                    // No special handling, just pass to super
            }
        } catch (Exception ex) {
            logger.debugf("Failed to persist attribute %s for user %s: %s", name, username, ex.getMessage());
            throw new ModelException("Failed to persist " + name + " into Cosmos DB.");
        }

        super.setAttribute(name, values);
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
        // Persist to Cosmos DB
        try {
            if (provider != null) {
                provider.updateUserNames(username, null, lastName);
            }
        } catch (Exception ex) {
            logger.debugf("Failed to persist lastName for user %s into Cosmos DB: %s", username, ex.getMessage());
            throw new ModelException("Failed to persist lastName into Cosmos DB.");
        }
        // Keep federated attribute for immediate UI consistency
        setSingleAttribute("lastName", (lastName == null || lastName.isBlank()) ? null : lastName);
    }

    @Override
    public boolean isEnabled() {
        return itemDoc != null && itemDoc.has("Active") && itemDoc.get("Active").asInt() == 1;
    }

    @Override
    public void setEnabled(boolean enabled) {
        try {
            if (provider != null) {
                provider.updateActive(username, enabled);
            }
        } catch (Exception ex) {
            logger.debugf("Failed to persist enabled for user %s into Cosmos DB: %s", username, ex.getMessage());
            throw new ModelException("Failed to persist enabled into Cosmos DB.");
        }
        // no federated storage for enabled flag here
    }

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
            case "CompanyId":
                String tou = getCompanyId();
                return tou == null ? Stream.empty() : Stream.of(tou);
            case "userLWPId":
            case "UserLWPId":
                String dou = getUserLWPId();
                return dou == null ? Stream.empty() : Stream.of(dou);
            default:
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
        String tou = getCompanyId(); if (tou != null) { m.put("companyId", List.of(tou)); }
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

    private Long createdTimestamp = null;

    @Override
    public Long getCreatedTimestamp() {
        if (createdTimestamp != null) return createdTimestamp;

       try {
            // Prefer Header.TimeCreation (ISO OffsetDateTime string)
            if (headerDoc != null && headerDoc.has("TimeCreation") && !headerDoc.get("TimeCreation").isNull()) {
                String ts = headerDoc.get("TimeCreation").asText(null);
                if (ts != null && !ts.isBlank()) {
                    createdTimestamp = OffsetDateTime.parse(ts).toInstant().toEpochMilli();
                    return createdTimestamp;
                }
            }
        } catch (Exception e) {
            logger.debugf("Could not parse created timestamp: %s", e.getMessage());
        }

        return null;
    }
    @Override
    public void setCreatedTimestamp(Long timestamp) {
        this.createdTimestamp = timestamp;
    }

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
