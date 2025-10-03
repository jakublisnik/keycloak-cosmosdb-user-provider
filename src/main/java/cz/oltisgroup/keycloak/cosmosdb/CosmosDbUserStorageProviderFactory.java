package cz.oltisgroup.keycloak.cosmosdb;

import org.keycloak.component.ComponentModel;
import org.keycloak.models.KeycloakSession;
import org.keycloak.provider.ProviderConfigProperty;
import org.keycloak.provider.ProviderConfigurationBuilder;
import org.keycloak.storage.UserStorageProviderFactory;

import java.util.List;

public class CosmosDbUserStorageProviderFactory implements UserStorageProviderFactory<CosmosDbUserStorageProvider> {

    public static final String PROVIDER_NAME = "cosmosdb-user-provider";
    public static final String ENDPOINT = "endpoint";
    public static final String KEY = "key";
    public static final String DATABASE_NAME = "databaseName";
    public static final String CONTAINER_NAME = "containerName";
    public static final String CLIENT_KEEP_ALIVE_SECONDS = "clientKeepAliveSeconds";

    @Override
    public CosmosDbUserStorageProvider create(KeycloakSession session, ComponentModel model) {
        return new CosmosDbUserStorageProvider(session, model);
    }

    @Override
    public String getId() {
        return PROVIDER_NAME;
    }

    @Override
    public List<ProviderConfigProperty> getConfigProperties() {
        return ProviderConfigurationBuilder.create()
                .property()
                .name(ENDPOINT)
                .label("Cosmos DB Endpoint")
                .type(ProviderConfigProperty.STRING_TYPE)
                .helpText("Cosmos DB Account Endpoint URL")
                .add()
                .property()
                .name(KEY)
                .label("Cosmos DB Key")
                .type(ProviderConfigProperty.STRING_TYPE)
                .helpText("Cosmos DB Account Primary Key")
                .add()
                .property()
                .name(DATABASE_NAME)
                .label("Database Name")
                .type(ProviderConfigProperty.STRING_TYPE)
                .helpText("Název databáze v Cosmos DB")
                .add()
                .property()
                .name(CONTAINER_NAME)
                .label("Container Name")
                .type(ProviderConfigProperty.STRING_TYPE)
                .helpText("Název kontejneru s uživateli")
                .defaultValue("User")
                .add()
                .property()
                .name(CLIENT_KEEP_ALIVE_SECONDS)
                .label("Client Keep-Alive (s)")
                .type(ProviderConfigProperty.STRING_TYPE)
                .defaultValue("30")
                .helpText("Počet sekund po uvolnění poslední reference, po které zůstane CosmosClient otevřen (0 = okamžité zavření)")
                .add()
                .build();
    }
}
