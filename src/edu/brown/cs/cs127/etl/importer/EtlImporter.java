package edu.brown.cs.cs127.etl.importer;
import java.io.*;
import java.util.*;
import au.com.bytecode.opencsv.CSVReader; //for the CSVReader objects
import java.sql.*;
import java.text.*;
import java.util.Date;


class normalize_data
{
	public String normalize_date_time(String input_date, String input_time) throws Exception
	{

		DateFormat standardDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		DateFormat[] sampleDateFormats = {new SimpleDateFormat("MM-dd-yyyy"),
									  	  new SimpleDateFormat("MM/dd/yyyy"),
									  	  new SimpleDateFormat("yyyy/MM/dd")};

		String date = null;

		try {
			standardDateFormat.setLenient(false);
			Date sampledateParsed = standardDateFormat.parse(input_date);
			date = standardDateFormat.format(sampledateParsed);
		} catch (ParseException e) {
		}

		for(int i = 0; i < 3; i++) {
			try {
				sampleDateFormats[i].setLenient(false);
				Date result = sampleDateFormats[i].parse(input_date);
				String sampledateNormalized = standardDateFormat.format(result);
				Date sampledateParsed = standardDateFormat.parse(sampledateNormalized);
				date = standardDateFormat.format(sampledateParsed);
				break;
			} catch (ParseException e) {

			}
		}

		DateFormat standardTimeFormat = new SimpleDateFormat("HH:mm");
		DateFormat sampleTimeFormat = new SimpleDateFormat("hh:mm a");
		sampleTimeFormat.setLenient(false);

		String time = null;

		try {
			Date result = sampleTimeFormat.parse(input_time);
			time = standardTimeFormat.format(result);

		} catch (ParseException e) {
			time = input_time;
		}

		String output = date+" "+time;
		return output;
	}

	}


class jdbc_connection
{
	public HashMap<String, Integer> airport_create_table(Statement stat, Connection conn, String AIRPORTS_FILE) throws Exception

	{

			stat.executeUpdate("CREATE TABLE airports (airport_id INTEGER PRIMARY KEY"
			   +" AUTOINCREMENT,"
				+"airport_code CHAR(3) UNIQUE,"+
				"airport_name VARCHAR(255), "+
				"city VARCHAR(255),"+
				"state VARCHAR(255)); ");


			PreparedStatement prep = conn.prepareStatement("INSERT OR IGNORE INTO airports"
			+" (airport_code, airport_name, city, state) VALUES (?, ?, ?,?)");
		  	CSVReader reader = new CSVReader(new FileReader(AIRPORTS_FILE));

			String[] nextLine;
			int count=1;
			HashMap<String, Integer>airportData = new HashMap<>();
			while ((nextLine = reader.readNext()) != null)
			   {
			    	prep.setString(1, nextLine[0]);
			    	airportData.put(nextLine[0], count);
			//    	System.out.println(airportData.get(nextLine[0]));
			     	prep.setString(2, nextLine[1]);
			     	prep.setString(3, "");
			     	prep.setString(4, "");
			     	count++;
			    	prep.addBatch();
			   }

			 conn.setAutoCommit(false);
			 prep.executeBatch();
			 conn.setAutoCommit(true);
			 reader.close();
			 prep.close();
			 return airportData;

	}

	public HashMap<String, Integer> airline_create_table(Statement stat, Connection conn, String AIRLINES_FILE) throws Exception
	{


			stat.executeUpdate("CREATE TABLE airlines (airline_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,"
			+"airline_code STRING UNIQUE,"
			+ "airline_name STRING); ");


			PreparedStatement prep = conn.prepareStatement("INSERT OR IGNORE INTO airlines (airline_code, airline_name) VALUES ( ?, ?)");
			CSVReader reader = new CSVReader(new FileReader(AIRLINES_FILE));

			String[] nextLine;
			int count=1;
			HashMap<String, Integer> airlineData= new HashMap<>();
			while ((nextLine = reader.readNext()) != null)
			{
				prep.setString(1, nextLine[0]);

				airlineData.put(nextLine[0], count);

			 	prep.setString(2, nextLine[1]);
			 	count++;
				prep.addBatch();
			}

			conn.setAutoCommit(false);
			prep.executeBatch();
			conn.setAutoCommit(true);
			reader.close();
			prep.close();
			return airlineData;

	}


	public void flights_create_table(Statement stat, Connection conn, String FLIGHTS_FILE, HashMap<String, Integer> airportData, HashMap<String, Integer> airlineData) throws Exception
	{

		stat.executeUpdate("CREATE TABLE flights("
			+"flights_id INTEGER PRIMARY KEY AUTOINCREMENT,"
			+"airline_id INTEGER,"
			+"flight_num INTEGER NOT NULL," //all columns have a flight number (485251)
			+"origin_airport_id INTEGER NOT NULL," //all coloumsn have orign and dest id and they follow the check below (485252)
			+"dest_airport_id INTEGER NOT NULL CHECK(dest_airport_id!=origin_airport_id),"
			+"departure_dt DATETIME,"
			+"depart_diff INTEGER,"
			+"arrival_dt DATETIME CHECK(arrival_dt>departure_dt),"
			+"arrival_diff INTEGER  CHECK(strftime('%Y-%m-%d %H:%M', departure_dt, depart_diff || ' minute')<strftime('%Y-%m-%d %H:%M', arrival_dt, arrival_diff || ' minute')),"
			+"cancelled BOOLEAN,"
			+"carrier_delay INTEGER CHECK(carrier_delay>=0),"
			+"weather_delay INTEGER CHECK(weather_delay>=0),"
			+"air_traffic_delay INTEGER CHECK(air_traffic_delay>=0),"
			+"security_delay INTEGER CHECK(security_delay>=0),"
			+"FOREIGN KEY(airline_id) REFERENCES airlines(airline_id),"
			+"FOREIGN KEY(origin_airport_id) REFERENCES  airports(airport_id),"
			+"FOREIGN KEY(dest_airport_id) REFERENCES airports(airport_id)"
			+");");

		PreparedStatement prep = conn.prepareStatement("INSERT OR IGNORE INTO flights (airline_id,"
			+"flight_num,"
			+"origin_airport_id,"
			+"dest_airport_id,"
			+"departure_dt,"
			+"depart_diff,"
			+"arrival_dt,"
			+"arrival_diff,"
			+"cancelled,"
			+"carrier_delay,"
			+"weather_delay,"
			+"air_traffic_delay,"
			+"security_delay) VALUES (?, ?, ?, ?, ?, ?, ? , ? , ?, ? , ?, ?, ?)");

		PreparedStatement prep2 = conn.prepareStatement("UPDATE airports SET city=?, state=? WHERE airport_id=?;");
		CSVReader reader = new CSVReader(new FileReader(FLIGHTS_FILE));

		String[] nextLine;
		normalize_data nd = new normalize_data();
		while ((nextLine = reader.readNext()) != null)
		   {
		   		//Step 1: Get all data from flights.csv
		   		String airline_code = nextLine[0];
		   		int flight_num = Integer.parseInt(nextLine[1]);
		   		String origin_airport_code = nextLine[2]; //Here origin is where the flight departs from
		   		String origin_city  = nextLine[3];
		   		String origin_state =  nextLine[4];
		   		String dest_airport_code = nextLine[5];
		   		String dest_city= nextLine[6];
		   		String dest_state = nextLine[7];
		   	    String departure_dt = nextLine[8];
		   	    String depart_time = nextLine[9];
		   	    int depart_diff = Integer.parseInt(nextLine[10]);
		   	    String arrive_date = nextLine[11];
		   	    String arrive_time = nextLine[12];
		   	    int arrive_delay = Integer.parseInt(nextLine[13]);
		   	    int cancelled = Integer.parseInt(nextLine[14]);
		   	    int carrier_delay = Integer.parseInt(nextLine[15]);
		   	    int weather_delay = Integer.parseInt(nextLine[16]);
		   	    int air_traffic_delay = Integer.parseInt(nextLine[17]);
		   	    int security_delay = Integer.parseInt(nextLine[18]);


		   	    //Setting the Airline ID
		   	    if(airlineData.containsKey(airline_code))
		   	    {
		   	    	prep.setInt(1, airlineData.get(airline_code));
		   	    }
		   	    else {continue;}

		   	    //Set Flight Number
		    	prep.setInt(2, flight_num);

		    	//Set origin_airport_id
		    	int origin_airport_id = 0;
		    	if(airportData.containsKey(origin_airport_code))
		   	    {
		   	    	origin_airport_id = airportData.get(origin_airport_code);
		   	    	prep.setInt(3, origin_airport_id);
		   	    }
		   	    else {continue;}

				//Updating the airports table
		   	    prep2.setString(1, origin_city);
		   	    prep2.setString(2, origin_state);
		   	    prep2.setInt(3, origin_airport_id );


		   	    //Set destination_airport_id
		   	    int dest_airport_id = 0;
		   	    if(airportData.containsKey(dest_airport_code))
		   	    {
		   	    	dest_airport_id = airportData.get(dest_airport_code);
		   	    	prep.setInt(4, dest_airport_id);
		   	    }
		   	    else {continue;}

		   	    //Updating the airports table

		   	    prep2.setString(1, dest_city);
		   	    prep2.setString(2, dest_state);
		   	    prep2.setInt(3, dest_airport_id);

		   	    //Set depart date
		   	    String departure_dt_time = nd.normalize_date_time(departure_dt, depart_time);
		   	    if (departure_dt_time!=null)
		   	    {
		   	    	prep.setString(5,departure_dt_time);
		   	    }
		   	    else{
		   	    	System.out.println("Date not set");
		   	    continue;}

		   	    //Set depart delay
		   	    prep.setInt(6, depart_diff);

		   	    //Set arrival date
		   	    String arrive_date_time = nd.normalize_date_time(arrive_date, arrive_time);
		   	    if (arrive_date!=null)
		   	    {
		   	    	prep.setString(7, arrive_date_time);
		   	    }
		   	    else{
		   	    	System.out.println("Date not set");
		   	    	continue;}

		   	    //Set arrival delay

		   	    prep.setInt(8, arrive_delay);

		   	    //Set Cancelled
		   	    prep.setInt(9, cancelled);

		   	    //Set Carrier Delay
		   	    prep.setInt(10, carrier_delay);

		   	    //Set Weather Delay
		   	    prep.setInt(11, weather_delay);

		   	    //Set Air Traffic Delay
		   	    prep.setInt(12, air_traffic_delay);

		   	    //Set Airport Security Delay
		   	    prep.setInt(13, security_delay);
		    	prep.addBatch();
		    	prep2.addBatch();
		   }

		  conn.setAutoCommit(false);
		  prep.executeBatch();
		  prep2.executeBatch();
		  conn.setAutoCommit(true);
		  reader.close();
		  prep.close();
		  prep2.close();


	}

}
public class EtlImporter
{
	public static void main(String[] args) throws Exception
	{
		if (args.length != 4)
		{
			System.err.println("This application requires exactly four parameters: " +
					"the path to the airports CSV, the path to the airlines CSV, " +
					"the path to the flights CSV, and the full path where you would " +
					"like the new SQLite database to be written to.");
			System.exit(1);
		}


		String AIRPORTS_FILE = args[0];
		String AIRLINES_FILE = args[1];
		String FLIGHTS_FILE = args[2];
		String DB_FILE = args[3];

		Class.forName("org.sqlite.JDBC");
		Connection conn = DriverManager.getConnection("jdbc:sqlite:" + DB_FILE);

		// ENABLE FOREIGN KEY CONSTRAINT CHECKING
		Statement stat = conn.createStatement();

		stat.executeUpdate("PRAGMA foreign_keys = ON;");


		// Speed up INSERTs
		stat.executeUpdate("PRAGMA synchronous = OFF;");
		stat.executeUpdate("PRAGMA journal_mode = MEMORY;");
		stat.executeUpdate("DROP TABLE IF EXISTS flights;");
		stat.executeUpdate("DROP TABLE IF EXISTS airports;");
		stat.executeUpdate("DROP TABLE IF EXISTS airlines;");


		jdbc_connection  jdbcc= new jdbc_connection();

		HashMap<String, Integer> airportData= jdbcc.airport_create_table(stat, conn, AIRPORTS_FILE);

		HashMap<String, Integer> airlineData= jdbcc.airline_create_table(stat, conn, AIRLINES_FILE);
		jdbcc.flights_create_table(stat, conn, FLIGHTS_FILE, airportData, airlineData);


		conn.close();
	}
}
