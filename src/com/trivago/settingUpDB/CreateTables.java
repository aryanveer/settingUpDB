package com.trivago.settingUpDB;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class CreateTables {
	
	public void create() {

		String url = "jdbc:mysql://localhost:3306/hotels";

		String username = "root";
		String password = "password";

		String cities = "CREATE TABLE IF NOT EXISTS cities(id INT NOT NULL, city_name VARCHAR(100) NOT NULL, PRIMARY KEY ( id ));";

		String advertisers = "CREATE TABLE IF NOT EXISTS advertisers(id INT NOT NULL, advertiser_name VARCHAR(100) NOT NULL, PRIMARY KEY ( id ));";

		String hotels = "CREATE TABLE IF NOT EXISTS hotels (id INT NOT NULL, city_id INT NOT NULL,"
				+ "clicks BIGINT NOT NULL,impressions BIGINT NOT NULL,name VARCHAR(200) NOT NULL,"
				+ "rating BIGINT NOT NULL, stars TINYINT NOT NULL, PRIMARY KEY (id), CONSTRAINT `city_foreign` FOREIGN KEY (`city_id`) REFERENCES cities(`id`),"
				+ "CONSTRAINT `chk_stars` CHECK ((`stars` >= 0) AND (`stars` <= 5)));";

		String hotel_advertiser = "CREATE TABLE IF NOT EXISTS hotel_advertiser (advertiser_id INT NOT NULL, hotel_id INT NOT NULL, "
				+ "cpc INT NOT NULL, price INT NOT NULL, currency VARCHAR(10) NOT NULL,"
				+ "availability_start_date INT NOT NULL, availability_end_date INT NOT NULL, FOREIGN KEY (`advertiser_id`) REFERENCES advertisers(`id`),"
				+ "FOREIGN KEY (`hotel_id`) REFERENCES hotels(`id`));";
		
		try (Connection conn = DriverManager.getConnection(url, username, password);
				Statement stmt = conn.createStatement()) {

			stmt.addBatch(cities);
			stmt.addBatch(advertisers);
			stmt.addBatch(hotels);
			stmt.addBatch(hotel_advertiser);
			stmt.executeBatch();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
