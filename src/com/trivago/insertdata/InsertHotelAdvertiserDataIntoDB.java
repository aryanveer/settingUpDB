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

public class InsertHotelAdvertiserDataIntoDB implements InsertDataIntoDB {

	public static class HotelAdvertiser {

		String advertiser_id;
		String hotel_id;
		String cpc;
		String price;
		String currency;
		String availability_start_date;
		String availability_end_date;

		public HotelAdvertiser() {
			super();
		}

		public HotelAdvertiser(String advertiser_id, String hotel_id, String cpc, String price, String currency,
				String availability_start_date, String availability_end_date) {
			super();
			this.advertiser_id = advertiser_id;
			this.hotel_id = hotel_id;
			this.cpc = cpc;
			this.price = price;
			this.currency = currency;
			this.availability_start_date = availability_start_date;
			this.availability_end_date = availability_end_date;
		}

		public String advertiser_id() {
			return advertiser_id;
		}

		public void setAdvertiser_id(String advertiser_id) {
			this.advertiser_id = advertiser_id;
		}

		public String hotel_id() {
			return hotel_id;
		}

		public void setHotel_id(String hotel_id) {
			this.hotel_id = hotel_id;
		}

		public String cpc() {
			return cpc;
		}

		public void setCpc(String cpc) {
			this.cpc = cpc;
		}

		public String price() {
			return price;
		}

		public void setPrice(String price) {
			this.price = price;
		}

		public String currency() {
			return currency;
		}

		public void setCurrency(String currency) {
			this.currency = currency;
		}

		public String availability_start_date() {
			return availability_start_date;
		}

		public void setAvailability_start_date(String availability_start_date) {
			this.availability_start_date = availability_start_date;
		}

		public String availability_end_date() {
			return availability_end_date;
		}

		public void setAvailability_end_date(String availability_end_date) {
			this.availability_end_date = availability_end_date;
		}

	}

	public void insertData() {

		String csvFilePath = "data/hotel_advertiser.csv";

		int batchSize = 20;

		Connection connection = null;

		ICsvBeanReader beanReader = null;
		CellProcessor[] processors = new CellProcessor[] { new NotNull(), new NotNull(), new NotNull(), new NotNull(),
				new NotNull(), new NotNull(), new NotNull(), };

		try {

			connection = DatabaseConnection.getConnection();
			connection.setAutoCommit(false);

			String sql = "INSERT INTO hotel_advertiser (advertiser_id, hotel_id, cpc, price, currency, availability_start_date, availability_end_date) VALUES (?, ?, ?, ?, ?, ?, ?)";
			PreparedStatement statement = connection.prepareStatement(sql);

			beanReader = new CsvBeanReader(new FileReader(csvFilePath), CsvPreference.STANDARD_PREFERENCE);

			String[] header = beanReader.getHeader(true); // skip header line
			HotelAdvertiser bean = null;

			int count = 0;

			while ((bean = beanReader.read(HotelAdvertiser.class, header, processors)) != null) {

				String advertiser_id = bean.advertiser_id();
				String hotel_id = bean.hotel_id();
				String cpc = bean.cpc();
				String price = bean.price();
				String currency = bean.currency();
				String availability_start_date = bean.availability_start_date();
				String availability_end_date = bean.availability_end_date();

				statement.setString(1, advertiser_id);
				statement.setString(2, hotel_id);
				statement.setString(3, cpc);
				statement.setString(4, price);
				statement.setString(5, currency);
				statement.setString(6, availability_start_date);
				statement.setString(7, availability_end_date);

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
