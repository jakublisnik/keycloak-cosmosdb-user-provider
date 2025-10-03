# Keycloak CosmosDB User Provider

This project implements a custom Keycloak User Storage Provider backed by Azure Cosmos DB. It allows Keycloak to authenticate and manage users stored in a Cosmos DB container.

## Features

- Integrates Keycloak with Azure Cosmos DB for user storage
- Configurable connection parameters (endpoint, key, database, container)
- Supports user management via Keycloak admin console

## Prerequisites

- Java 11 or newer
- Maven 3.6+
- Access to an Azure Cosmos DB account

### How to install Maven (Windows)

1. Download Maven from the official website: https://maven.apache.org/download.cgi
2. Extract the archive to a folder, e.g. `C:\maven`
3. Add the `bin` directory to your system PATH:
   - Open Control Panel > System > Advanced > Environment Variables
   - Edit the `Path` variable and add: `C:\maven\bin`
4. Set the `MAVEN_HOME` environment variable to the Maven folder (optional):
   - Add new variable: `MAVEN_HOME = C:\maven`
5. Open a new command prompt and verify installation:
   ```
   mvn -version
   ```
   You should see Maven version info.

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/jakublisnik/keycloak-cosmosdb-user-provider.git
   cd keycloak-cosmosdb-user-provider
   ```

2. Build the project:
   ```
   mvn clean package
   ```

   This will generate a JAR file in the `target` directory.

## Usage

1. Copy the generated JAR from `target` to your Keycloak server's `providers` directory.

2. Restart Keycloak.

3. In the Keycloak admin console, add a new User Storage Provider and select "cosmosdb-user-provider".

4. Configure the connection properties (endpoint, key, database name, container name, etc.).

## Configuration

The following properties can be set in the Keycloak admin console:

- Cosmos DB Endpoint
- Cosmos DB Key
- Database Name
- Container Name
- Client Keep-Alive (seconds)

## License

MIT
