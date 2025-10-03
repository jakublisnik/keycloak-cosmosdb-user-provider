package cz.oltisgroup.keycloak.cosmosdb;

import com.azure.cosmos.*;
import com.azure.cosmos.models.*;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestCosmosDbProvider {

    public static void main(String[] args) {
        // Test configuration
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
            System.out.println("=== Cosmos DB Connection Test ===");

            // Connect
            cosmosClient = new CosmosClientBuilder()
                    .endpoint(endpoint)
                    .key(key)
                    .consistencyLevel(ConsistencyLevel.SESSION)
                    .buildClient();

            CosmosDatabase database = cosmosClient.getDatabase(databaseName);
            CosmosContainer container = database.getContainer(containerName);

            System.out.println("✅ Connection successful!");

            // Test 1: Document count
            testDocumentCount(container);

            // Test 2: Find user by UserAdId
            testFindUserByUsername(container, "girmalar");

            // Test 3: Find user by email
            testFindUserByEmail(container, "testgoogle@google.com");

            // Test 4: List all active users
            testListActiveUsers(container);

            // Test 5: Password validation
            testPasswordValidation(container, "girmalar", "bgggggggg1!");

        } catch (Exception e) {
            System.err.println("❌ Error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (cosmosClient != null) {
                cosmosClient.close();
            }
        }
    }

    private static void testDocumentCount(CosmosContainer container) {
        try {
            System.out.println("\n=== Test: Document Count ===");
            String query = "SELECT VALUE COUNT(1) FROM c";
            CosmosPagedIterable<Integer> queryResponse = container.queryItems(
                    query,
                    new CosmosQueryRequestOptions(),
                    Integer.class
            );

            for (Integer count : queryResponse) {
                System.out.println("Total documents: " + count);
            }

            // Count of active users
            String activeQuery = "SELECT VALUE COUNT(1) FROM c WHERE c.Item.Active = 1";
            CosmosPagedIterable<Integer> activeResponse = container.queryItems(
                    activeQuery,
                    new CosmosQueryRequestOptions(),
                    Integer.class
            );

            for (Integer count : activeResponse) {
                System.out.println("Active users: " + count);
            }

        } catch (Exception e) {
            System.err.println("❌ Error counting documents: " + e.getMessage());
        }
    }

    private static void testFindUserByUsername(CosmosContainer container, String username) {
        try {
            System.out.println("\n=== Test: Find user by UserAdId ===");
            String query = "SELECT * FROM c WHERE c.Header.UserAdId = '" + username + "'";

            CosmosPagedIterable<JsonNode> queryResponse = container.queryItems(
                    query,
                    new CosmosQueryRequestOptions(),
                    JsonNode.class
            );


            boolean found = false;
            for (JsonNode userDoc : queryResponse) {
                found = true;
                System.out.println("✅ User found: " + username);
                printUserInfo(userDoc);
            }

            if (!found) {
                System.out.println("❌ User not found: " + username);
            }

        } catch (Exception e) {
            System.err.println("❌ Error finding user: " + e.getMessage());
        }
    }

    private static void testFindUserByEmail(CosmosContainer container, String email) {
        try {
            System.out.println("\n=== Test: Find user by email ===");
            String query = "SELECT * FROM c WHERE c.Item.Email = '" + email + "'";

            CosmosPagedIterable<JsonNode> queryResponse = container.queryItems(
                    query,
                    new CosmosQueryRequestOptions(),
                    JsonNode.class
            );

            boolean found = false;
            for (JsonNode userDoc : queryResponse) {
                found = true;
                System.out.println("✅ User found by email: " + email);
                printUserInfo(userDoc);
            }

            if (!found) {
                System.out.println("ℹ️ User not found by email: " + email);
            }

        } catch (Exception e) {
            System.err.println("❌ Error finding by email: " + e.getMessage());
        }
    }

    private static void testListActiveUsers(CosmosContainer container) {
        try {
            System.out.println("\n=== Test: List active users ===");
            String query = "SELECT * FROM c WHERE c.Item.Active = 1";

            CosmosPagedIterable<JsonNode> queryResponse = container.queryItems(
                    query,
                    new CosmosQueryRequestOptions(),
                    JsonNode.class
            );

            int count = 0;
            for (JsonNode userDoc : queryResponse) {
                count++;
                System.out.println("User #" + count + ":");
                printUserInfo(userDoc);
                System.out.println("---");
            }

            System.out.println("Total active users: " + count);

        } catch (Exception e) {
            System.err.println("❌ Error listing users: " + e.getMessage());
        }
    }

    private static void testPasswordValidation(CosmosContainer container, String username, String password) {
        try {
            System.out.println("\n=== Test: Password validation ===");
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

                    System.out.println("User: " + username);
                    System.out.println("Entered password: " + password);
                    System.out.println("Stored password: " + storedPassword);
                    System.out.println("Validation: " + (isValid ? "✅ SUCCESS" : "❌ INVALID"));
                    return;
                }
            }

            System.out.println("❌ User not found for password validation: " + username);

        } catch (Exception e) {
            System.err.println("❌ Error validating password: " + e.getMessage());
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
                System.out.println("  First name: " + item.get("Name").asText());
                System.out.println("  Last name: " + item.get("Surename").asText());
                System.out.println("  Email: " + item.get("Email").asText());
                System.out.println("  Role: " + item.get("Role").asText());
                System.out.println("  Active: " + (item.get("Active").asInt() == 1 ? "Yes" : "No"));
                if (item.has("CompanyId")) {
                    System.out.println("  CompanyId (Item): " + item.get("CompanyId").asText());
                    System.out.println("  typeOfUser (for Keycloak): " + item.get("CompanyId").asText());
                }
            }

        } catch (Exception e) {
            System.out.println("  Error printing info: " + e.getMessage());
        }
    }
}
