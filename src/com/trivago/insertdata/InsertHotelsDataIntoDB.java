package com.trivago.insertdata;

import java.io.FileReader;
import java.io.IOException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.ICsvBeanReader;
import org.supercsv.prefs.CsvPreference;

import com.trivago.settingUpDB.DatabaseConnection;

public class InsertHotelsDataIntoDB implements InsertDataIntoDB {

	public static class Hotels {

		String id;
		String city_id;
		String clicks;
		String impressions;
		String name;
		String rating;
		String stars;

		public Hotels() {
			super();
		}

		public Hotels(String id, String city_id, String clicks, String impressions, String name, String rating,
				String stars) {
			super();
			this.id = id;
			this.city_id = city_id;
			this.clicks = clicks;
			this.impressions = impressions;
			this.name = name;
			this.rating = rating;
			this.stars = stars;
		}

		public String clicks() {
			return clicks;
		}

		public void setClicks(String clicks) {
			this.clicks = clicks;
		}

		public String impressions() {
			return impressions;
		}

		public void setImpressions(String impressions) {
			this.impressions = impressions;
		}

		public String name() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String rating() {
			return rating;
		}

		public void setRating(String rating) {
			this.rating = rating;
		}

		public String stars() {
			return stars;
		}

		public void setStars(String stars) {
			this.stars = stars;
		}

		public String id() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String cityId() {
			return city_id;
		}

		public void setCity_Id(String city_id) {
			this.city_id = city_id;
		}

	}

	public void insertData() {

		String csvFilePath = "data/hotels.csv";

		int batchSize = 20;

		Connection connection = null;

		ICsvBeanReader beanReader = null;
		CellProcessor[] processors = new CellProcessor[] { new NotNull(), // id
				new NotNull(), // name
				new NotNull(), new NotNull(), new NotNull(), new NotNull(), new NotNull(), };

		try {

			connection = DatabaseConnection.getConnection();
			connection.setAutoCommit(false);

			String sql = "INSERT INTO hotels (id, city_id, clicks, impressions, name, rating, stars) VALUES (?, ?, ?, ?, ?, ?, ?)";
			PreparedStatement statement = connection.prepareStatement(sql);

			beanReader = new CsvBeanReader(new FileReader(csvFilePath), CsvPreference.STANDARD_PREFERENCE);

			String[] header = beanReader.getHeader(true); // skip header line
			Hotels bean = null;

			int count = 0;

			while ((bean = beanReader.read(Hotels.class, header, processors)) != null) {

				String id = bean.id();
				String city_id = bean.cityId();
				String clicks = bean.clicks();
				String impressions = bean.impressions();
				String name = bean.name();
				String rating = bean.rating();
				String stars = bean.stars();

				statement.setString(1, id);
				statement.setString(2, city_id);
				statement.setString(3, clicks);
				statement.setString(4, impressions);
				statement.setString(5, name);
				statement.setString(6, rating);
				statement.setString(7, stars);

				statement.addBatch();

				if (count % batchSize == 0) {
					statement.executeBatch();
				}
				count += 1;
			}

			beanReader.close();

			// execute the remaining queries
			statement.executeBatch();

			connection.commit();
			connection.close();

		} catch (IOException | BatchUpdateException ex) {
			System.err.println(ex);
		} catch (SQLException ex) {
			ex.printStackTrace();

			try {
				connection.rollback();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

	}

}
