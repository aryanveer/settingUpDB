package com.trivago.insertdata;

import java.io.FileReader;
import java.io.IOException;
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

public class InsertCitiesDataIntoDB implements InsertDataIntoDB{

	public static class Cities {

		String id;
		String city_name;

		public Cities() {
			super();
		}

		public Cities(String id, String city_name) {
			super();
			this.id = id;
			this.city_name = city_name;
		}

		public String id() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String cityName() {
			return city_name;
		}

		public void setCity_Name(String city_name) {
			this.city_name = city_name;
		}

	}

	public void insertData() {

		

		String csvFilePath = "data/cities.csv";

		int batchSize = 20;

		Connection connection = null;

		ICsvBeanReader beanReader = null;
		CellProcessor[] processors = new CellProcessor[] { new NotNull(), // id
				new NotNull(), // name
		};

		try {

			connection = DatabaseConnection.getConnection();
			connection.setAutoCommit(false);

			String sql = "INSERT INTO cities (id, city_name) VALUES (?, ?)";
			PreparedStatement statement = connection.prepareStatement(sql);

			beanReader = new CsvBeanReader(new FileReader(csvFilePath), CsvPreference.STANDARD_PREFERENCE);

			String[] header = beanReader.getHeader(true); // skip header line
			Cities bean = null;

			int count = 0;

			while ((bean = beanReader.read(Cities.class, header, processors)) != null) {

				String id = bean.id();
				String city_name = bean.cityName();

				statement.setString(1, id);
				statement.setString(2, city_name);

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

		} catch (IOException ex) {
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
