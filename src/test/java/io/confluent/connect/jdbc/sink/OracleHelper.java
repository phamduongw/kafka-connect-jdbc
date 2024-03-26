package io.confluent.connect.jdbc.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.Assert.fail;

public final class OracleHelper {
    public Connection connection;

    public String oracleUri() {
        return "jdbc:oracle:thin:T24BNK/n4ngC#pth3K@dc2uatdbdev.seauat.com.vn:1521/ODS_DEV";
    }

    public void setUp() {
        try {
            connection = DriverManager.getConnection(oracleUri());
        } catch (SQLException e) {
            fail("Failed to establish connection: " + e.getMessage());
        }
    }

    public void tearDown() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                fail("Failed to close connection: " + e.getMessage());
            }
        }
    }
}
