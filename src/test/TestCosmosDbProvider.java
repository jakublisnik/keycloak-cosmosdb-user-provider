package cz.oltisgroup.keycloak.cosmosdb;

import com.azure.cosmos.*;
import com.azure.cosmos.models.*;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestCosmosDbProvider {

    public static void main(String[] args) {
        // Testovací konfigurace
        String endpoint = "databse_url";
        String key = "private_key";
        String databaseName = "db_name";
        String containerName = "db_container_name";

        testCosmosDbConnection(endpoint, key, databaseName, containerName);
    }

    public static void testCosmosDbConnection(String endpoint, String key,
                                              String databaseName, String containerName) {
        CosmosClient cosmosClient = null;

        try {
            System.out.println("=== Test připojení k Cosmos DB ===");

            // Připojení
            cosmosClient = new CosmosClientBuilder()
                    .endpoint(endpoint)
                    .key(key)
                    .consistencyLevel(ConsistencyLevel.SESSION)
                    .buildClient();

            CosmosDatabase database = cosmosClient.getDatabase(databaseName);
            CosmosContainer container = database.getContainer(containerName);

            System.out.println("✅ Připojení úspěšné!");

            // Test 1: Počet dokumentů
            testDocumentCount(container);

            // Test 2: Hledání uživatele podle UserAdId
            testFindUserByUsername(container, "girmalar");

            // Test 3: Hledání uživatele podle emailu
            testFindUserByEmail(container, "testgoogle@google.com");

            // Test 4: Seznam všech aktivních uživatelů
            testListActiveUsers(container);

            // Test 5: Validace hesla
            testPasswordValidation(container, "girmalar", "bgggggggg1!");

        } catch (Exception e) {
            System.err.println("❌ Chyba: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (cosmosClient != null) {
                cosmosClient.close();
            }
        }
    }

    private static void testDocumentCount(CosmosContainer container) {
        try {
            System.out.println("\n=== Test: Počet dokumentů ===");
            String query = "SELECT VALUE COUNT(1) FROM c";
            CosmosPagedIterable<Integer> queryResponse = container.queryItems(
                    query,
                    new CosmosQueryRequestOptions(),
                    Integer.class
            );

            for (Integer count : queryResponse) {
                System.out.println("Celkem dokumentů: " + count);
            }

            // Počet aktivních uživatelů
            String activeQuery = "SELECT VALUE COUNT(1) FROM c WHERE c.Item.Active = 1";
            CosmosPagedIterable<Integer> activeResponse = container.queryItems(
                    activeQuery,
                    new CosmosQueryRequestOptions(),
                    Integer.class
            );

            for (Integer count : activeResponse) {
                System.out.println("Aktivní uživatelé: " + count);
            }

        } catch (Exception e) {
            System.err.println("❌ Chyba při počítání dokumentů: " + e.getMessage());
        }
    }

    private static void testFindUserByUsername(CosmosContainer container, String username) {
        try {
            System.out.println("\n=== Test: Hledání uživatele podle UserAdId ===");
            String query = "SELECT * FROM c WHERE c.Header.UserAdId = '" + username + "'";

            CosmosPagedIterable<JsonNode> queryResponse = container.queryItems(
                    query,
                    new CosmosQueryRequestOptions(),
                    JsonNode.class
            );


            boolean found = false;
            for (JsonNode userDoc : queryResponse) {
                found = true;
                System.out.println("✅ Uživatel nalezen: " + username);
                printUserInfo(userDoc);
            }

            if (!found) {
                System.out.println("❌ Uživatel nenalezen: " + username);
            }

        } catch (Exception e) {
            System.err.println("❌ Chyba při hledání uživatele: " + e.getMessage());
        }
    }

    private static void testFindUserByEmail(CosmosContainer container, String email) {
        try {
            System.out.println("\n=== Test: Hledání uživatele podle emailu ===");
            String query = "SELECT * FROM c WHERE c.Item.Email = '" + email + "'";

            CosmosPagedIterable<JsonNode> queryResponse = container.queryItems(
                    query,
                    new CosmosQueryRequestOptions(),
                    JsonNode.class
            );

            boolean found = false;
            for (JsonNode userDoc : queryResponse) {
                found = true;
                System.out.println("✅ Uživatel nalezen podle emailu: " + email);
                printUserInfo(userDoc);
            }

            if (!found) {
                System.out.println("ℹ️ Uživatel nenalezen podle emailu: " + email);
            }

        } catch (Exception e) {
            System.err.println("❌ Chyba při hledání podle emailu: " + e.getMessage());
        }
    }

    private static void testListActiveUsers(CosmosContainer container) {
        try {
            System.out.println("\n=== Test: Seznam aktivních uživatelů ===");
            String query = "SELECT * FROM c WHERE c.Item.Active = 1";

            CosmosPagedIterable<JsonNode> queryResponse = container.queryItems(
                    query,
                    new CosmosQueryRequestOptions(),
                    JsonNode.class
            );

            int count = 0;
            for (JsonNode userDoc : queryResponse) {
                count++;
                System.out.println("Uživatel #" + count + ":");
                printUserInfo(userDoc);
                System.out.println("---");
            }

            System.out.println("Celkem aktivních uživatelů: " + count);

        } catch (Exception e) {
            System.err.println("❌ Chyba při výpisu uživatelů: " + e.getMessage());
        }
    }

    private static void testPasswordValidation(CosmosContainer container, String username, String password) {
        try {
            System.out.println("\n=== Test: Validace hesla ===");
            String query = "SELECT * FROM c WHERE c.Header.UserAdId = '" + username + "'";

            CosmosPagedIterable<JsonNode> queryResponse = container.queryItems(
                    query,
                    new CosmosQueryRequestOptions(),
                    JsonNode.class
            );

            for (JsonNode userDoc : queryResponse) {
                JsonNode item = userDoc.get("Item");
                if (item != null && item.has("Password")) {
                    String storedPassword = item.get("Password").asText();
                    boolean isValid = password.equals(storedPassword);

                    System.out.println("Uživatel: " + username);
                    System.out.println("Zadané heslo: " + password);
                    System.out.println("Uložené heslo: " + storedPassword);
                    System.out.println("Validace: " + (isValid ? "✅ ÚSPĚCH" : "❌ NEPLATNÉ"));
                    return;
                }
            }

            System.out.println("❌ Uživatel nenalezen pro validaci hesla: " + username);

        } catch (Exception e) {
            System.err.println("❌ Chyba při validaci hesla: " + e.getMessage());
        }
    }

    private static void printUserInfo(JsonNode userDoc) {
        try {
            JsonNode header = userDoc.get("Header");
            JsonNode item = userDoc.get("Item");

            if (header != null) {
                System.out.println("  UserAdId: " + header.get("UserAdId").asText());
                System.out.println("  CompanyId: " + header.get("CompanyId").asText());
            }

            if (item != null) {
                System.out.println("  Jméno: " + item.get("Name").asText());
                System.out.println("  Příjmení: " + item.get("Surename").asText());
                System.out.println("  Email: " + item.get("Email").asText());
                System.out.println("  Role: " + item.get("Role").asText());
                System.out.println("  Aktivní: " + (item.get("Active").asInt() == 1 ? "Ano" : "Ne"));
                if (item.has("CompanyId")) {
                    System.out.println("  CompanyId (Item): " + item.get("CompanyId").asText());
                    System.out.println("  typeOfUser (pro Keycloak): " + item.get("CompanyId").asText());
                }
            }

        } catch (Exception e) {
            System.out.println("  Chyba při výpisu info: " + e.getMessage());
        }
    }
}
