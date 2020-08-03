package com.trivago.settingUpDB;



import com.trivago.insertdata.InsertAdvertisersDataIntoDB;
import com.trivago.insertdata.InsertCitiesDataIntoDB;
import com.trivago.insertdata.InsertDataIntoDB;
import com.trivago.insertdata.InsertHotelAdvertiserDataIntoDB;
import com.trivago.insertdata.InsertHotelsDataIntoDB;

public class Main {

	public static void main(String[] args) {
		CreateDB db = new CreateDB();
		db.create();

		CreateTables tables = new CreateTables();
		tables.create();

		try{
		InsertDataIntoDB city = new InsertCitiesDataIntoDB();
		city.insertData();

		InsertDataIntoDB adv = new InsertAdvertisersDataIntoDB();
		adv.insertData();

		InsertDataIntoDB hotels = new InsertHotelsDataIntoDB();
		hotels.insertData();

		InsertDataIntoDB hotAdv = new InsertHotelAdvertiserDataIntoDB();
		hotAdv.insertData();
		}catch(RuntimeException e) {
			System.out.println("Cannot enter duplicate Data into the tables");
		}
	}
}
