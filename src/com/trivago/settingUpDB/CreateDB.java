package com.trivago.settingUpDB;

import java.sql.*;

public class CreateDB {
	
	public void create() {
        // Defines the JDBC URL. As you can see, we are not specifying
        // the database name in the URL.
        String url = "jdbc:mysql://localhost:3306";

        // Defines username and password to connect to database server.
        String username = "root";
        String password = "password";

        // SQL command to create a database in MySQL.
        String sql = "CREATE DATABASE IF NOT EXISTS hotels";

        try (Connection conn = DriverManager.getConnection(url, username, password);
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
