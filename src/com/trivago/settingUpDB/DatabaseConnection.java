package com.trivago.settingUpDB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public abstract class DatabaseConnection {
	
	static String jdbcURL = "jdbc:mysql://localhost:3306/hotels";
	static String username = "root";
	static String password = "password";
	
	public static Connection getConnection() {
		try {
			return DriverManager.getConnection(jdbcURL, username, password);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	

}
