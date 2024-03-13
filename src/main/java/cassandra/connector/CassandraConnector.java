package cassandra.connector;

import java.net.InetSocketAddress;
import java.util.Map;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlIdentifier;

import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.uuid.Uuids;

public class CassandraConnector {

    private CqlSession session;
    private String systemUserPrefix = "system_";
    private static final String NODE_IP = "127.0.0.1";
    private static final int PORT = 9042;
    private static final String KEYSPACE = "my_keyspace";
    private static final String LOCAL_DATACENTER = "datacenter1";
    private static final String INSERT_USER_QUERY = "INSERT INTO users (user_id, username, credential, role_list, claims, profile) VALUES (?, ?, ?, ?, ?, ?)";
    String createKeyspaceQuery = "CREATE KEYSPACE IF NOT EXISTS my_keyspace "
            + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};";

    String createTableQuery = "CREATE TABLE IF NOT EXISTS my_keyspace.users ("
            + "user_id UUID PRIMARY KEY, "
            + "username TEXT, "
            + "credential BLOB, "
            + "role_list SET<TEXT>, "
            + "claims MAP<TEXT, TEXT>, "
            + "profile TEXT)";
    public void connect(String node, Integer port, String dataCenter) {
        CqlSessionBuilder builder = CqlSession.builder();
        builder.addContactPoint(new InetSocketAddress(node, port));
        builder.withLocalDatacenter(dataCenter);

        this.session = builder.build();

        System.out.println("Connected to Cassandra");
    }

    public void useKeyspace(String keyspace) {
        session.execute("USE " + CqlIdentifier.fromCql(keyspace));
    }

    public static void main(String[] args) {
        System.out.println("Connecting Cassandra");
        CassandraConnector connector = new CassandraConnector();
        try {
            connector.connect(NODE_IP, PORT, LOCAL_DATACENTER);}
        catch (DriverException e) {
            System.out.println("Error connecting to Cassandra");
            e.printStackTrace();
        }

        try {
            connector.session.execute(connector.createKeyspaceQuery);
            System.out.println("Keyspace created");
        }
        catch (DriverException e) {
            System.out.println("Error creating keyspace");
            e.printStackTrace();
        }


    }


//        connect();

        // try (CqlSession session = CqlSession.builder()
        // .addContactPoint(new InetSocketAddress(NODE_IP, PORT))
        // .withKeyspace(KEYSPACE)
        // .build()) {
        //     // Generate a unique UUID for the user
        //     java.util.UUID userId = Uuids.timeBased();

        //     // Convert roleList array to a set
        //     HashSet<String> roleSet = arrayToSet(roleList);

        //     // Prepare the insert statement
        //     PreparedStatement preparedStatement = session.prepare(INSERT_USER_QUERY);

        //     // Execute the insert statement
        //     session.execute(preparedStatement.bind(
        //             userId,                // user_id
        //             userName,             // username
        //             credential.toString(),// credential
        //             roleSet,              // role_list
        //             claims,               // claims
        //             profile));            // profile

        //     // Print success message
        //     System.out.println("User added successfully to Cassandra database");
        // } catch (Exception e) {
        //     e.printStackTrace();
        //     return false;
        // }

}
