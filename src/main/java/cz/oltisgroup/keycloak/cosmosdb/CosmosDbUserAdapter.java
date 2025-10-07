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
            throw new ModelException("Nepodařilo se uložit data do Cosmos DB. Uživatel nebyl vytvořen.");
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
        if ("firstName".equals(name) || "lastName".equals(name)) {
            try {
                if (provider != null) {
                    String fn = "firstName".equals(name) ? value : null;
                    String ln = "lastName".equals(name) ? value : null;
                    provider.updateUserNames(username, fn, ln);
                }
            } catch (Exception ex) {
                logger.debugf("Failed to persist attribute %s for user %s: %s", name, username, ex.getMessage());
                throw new ModelException("Failed to persist " + name + " into Cosmos DB.");
            }
        } else if ("companyId".equalsIgnoreCase(name) || "userLWPId".equalsIgnoreCase(name) || "typeOfUser".equalsIgnoreCase(name)) {
            try {
                if (provider != null) {
                    String cid = (name.equalsIgnoreCase("companyId") || name.equalsIgnoreCase("typeOfUser")) ? value : null;
                    String lid = name.equalsIgnoreCase("userLWPId") ? value : null;
                    provider.updateHeaderAttributes(username, cid, lid);
                }
            } catch (Exception ex) {
                logger.debugf("Failed to persist header attribute %s for user %s: %s", name, username, ex.getMessage());
                throw new ModelException("Failed to persist " + name + " into Cosmos DB.");
            }
        } else if ("email".equalsIgnoreCase(name)) {
            try {
                if (provider != null) {
                    provider.updateEmail(username, value);
                }
            } catch (Exception ex) {
                logger.debugf("Failed to persist email via setSingleAttribute for user %s: %s", username, ex.getMessage());
                throw new ModelException("Failed to persist email into Cosmos DB.");
            }
        }
        super.setSingleAttribute(name, value);
    }

    @Override
    public void setAttribute(String name, List<String> values) {
        if (("firstName".equals(name) || "lastName".equals(name)) && values != null) {
            String v = values.isEmpty() ? null : values.get(0);
            try {
                if (provider != null) {
                    String fn = "firstName".equals(name) ? v : null;
                    String ln = "lastName".equals(name) ? v : null;
                    provider.updateUserNames(username, fn, ln);
                }
            } catch (Exception ex) {
                logger.debugf("Failed to persist attribute(list) %s for user %s: %s", name, username, ex.getMessage());
                throw new ModelException("Failed to persist " + name + " into Cosmos DB.");
            }
        } else if ("companyId".equalsIgnoreCase(name) || "userLWPId".equalsIgnoreCase(name)  && values != null) {
            String v = values.isEmpty() ? null : values.get(0);
            try {
                if (provider != null) {
                    String cid = name.equalsIgnoreCase("companyId")  ? v : null;
                    String lid = name.equalsIgnoreCase("userLWPId") ? v : null;
                    provider.updateHeaderAttributes(username, cid, lid);
                }
            } catch (Exception ex) {
                logger.debugf("Failed to persist header attribute(list) %s for user %s: %s", name, username, ex.getMessage());
                throw new ModelException("Failed to persist " + name + " into Cosmos DB.");
            }
        } else if ("email".equalsIgnoreCase(name) && values != null) {
            String v = values.isEmpty() ? null : values.get(0);
            try {
                if (provider != null) {
                    provider.updateEmail(username, v);
                }
            } catch (Exception ex) {
                logger.debugf("Failed to persist email via setAttribute for user %s: %s", username, ex.getMessage());
                throw new ModelException("Failed to persist email into Cosmos DB.");
            }
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
