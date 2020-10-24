package edu.brown.cs.cs127.etl.query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.text.*;


public class EtlQuery
{
	private Connection conn;

	public EtlQuery(String pathToDatabase) throws Exception
	{
		Class.forName("org.sqlite.JDBC");
		conn = DriverManager.getConnection("jdbc:sqlite:" + pathToDatabase);

		Statement stat = conn.createStatement();
		stat.executeUpdate("PRAGMA foreign_keys = ON;");
	}

	public ResultSet queryA(String[] args) throws SQLException
	{
		/**
		 * For some sample JDBC code, check out
		 * http://web.archive.org/web/20100814175321/http://www.zentus.com/sqlitejdbc/
		 */
		PreparedStatement stat = conn.prepareStatement(
			"SELECT COUNT(airport_code) FROM airports;"
		);
		return stat.executeQuery();
	}

	public ResultSet queryB(String[] args) throws SQLException
	{
		/**
		 * For some sample JDBC code, check out
		 * http://web.archive.org/web/20100814175321/http://www.zentus.com/sqlitejdbc/
		 */
		PreparedStatement stat = conn.prepareStatement(
			"SELECT COUNT(airline_code) FROM airlines;"
		);
		return stat.executeQuery();
	}

		public ResultSet queryC(String[] args) throws SQLException
	{
		/**
		 * For some sample JDBC code, check out
		 * http://web.archive.org/web/20100814175321/http://www.zentus.com/sqlitejdbc/
		 */
		PreparedStatement stat = conn.prepareStatement(
			"SELECT COUNT(flights_id) FROM flights;"
		);
		return stat.executeQuery();
	}

	public ResultSet query0(String[] args) throws SQLException
	{

		PreparedStatement stat = conn.prepareStatement(
			"SELECT airport_name, city, state FROM airports WHERE state='Alaska' ORDER BY city desc LIMIT 3;"
		);

		return stat.executeQuery();
	}

	public ResultSet query1(String[] args) throws SQLException
	{
		/**
		 * For some sample JDBC code, check out
		 * http://web.archive.org/web/20100814175321/http://www.zentus.com/sqlitejdbc/
		 */
		PreparedStatement stat = conn.prepareStatement(
			"SELECT airline_name, MIN(flight_num) FROM flights,airlines WHERE airline_name = ? AND flights.airline_id = airlines.airline_id;"
		);
		stat.setString(1, args[0]);
		return stat.executeQuery();
	}

	public ResultSet query2(String[] args) throws SQLException
	{
		/**
		 * For some sample JDBC code, check out
		 * http://web.archive.org/web/20100814175321/http://www.zentus.com/sqlitejdbc/
		 */
		PreparedStatement stat = conn.prepareStatement(
			"SELECT COUNT(cancelled) FROM flights,airlines WHERE airline_name = ? AND flights.airline_id = airlines.airline_id AND cancelled=1;"
		);
		stat.setString(1, args[0]);
		return stat.executeQuery();
	}

	public ResultSet query3(String[] args) throws SQLException
	{
		PreparedStatement stat = conn.prepareStatement("SELECT airport_name, COUNT(flights_id) FROM airports, flights WHERE origin_airport_id = airport_id GROUP BY airport_name HAVING COUNT(flights_id) > 10000 ORDER BY COUNT(flights_id) DESC LIMIT 5;");
		return stat.executeQuery();
	}


		public ResultSet query4(String[] args) throws SQLException
	{
		PreparedStatement stat = conn.prepareStatement(
			"SELECT 'Air Traffic Delay' AS delayType, COUNT(air_traffic_delay) AS delayCount\n"
			+"FROM flights WHERE air_traffic_delay > 0 UNION\n"
			+"SELECT 'Carrier Delay' AS delayType, COUNT(carrier_delay) AS delayCount\n"
			+"FROM flights WHERE carrier_delay > 0 UNION\n"
			+"SELECT 'Security Delay' AS delayType, COUNT(security_delay) AS delayCount\n"
			+"FROM flights WHERE security_delay > 0 UNION\n"
			+"SELECT 'Weather Delay' AS delayType, COUNT(weather_delay) AS delayCount\n"
			+"FROM flights WHERE weather_delay > 0\n"
			+"ORDER BY delayCount DESC; "
			);



		return stat.executeQuery();
	}


	public ResultSet query5(String[] args) throws SQLException
	{

		String date = args[0] + "/" + args[1] + "/" + args[2];//MM/dd/yy
		String final_date = null;
		try{
			final_date = normalize_date_time(date);
		} catch(Exception e) {}

		PreparedStatement stat = conn.prepareStatement(
			"SELECT airline_name, COUNT(flights_id)\n"
			+"FROM airlines\n"
			+"LEFT OUTER JOIN flights ON airlines.airline_id = flights.airline_id AND date(flights.departure_dt)=?\n"
			+"GROUP BY airline_name\n"
			+"ORDER BY COUNT(flights_id) DESC, airline_name ASC;"
			);
		stat.setString(1, final_date);
		return stat.executeQuery();
	}


	public ResultSet query6(String[] args) throws SQLException
	{

		String date = args[0] + "/" + args[1] + "/" + args[2];//MM/dd/yy
		String final_date = null;
		try{
			final_date = normalize_date_time(date);
		} catch(Exception e) {}

		PreparedStatement stat = conn.prepareStatement (
			"WITH departFlights AS (\n"
			+"SELECT airport_name, COUNT(F1.flights_id) as departCount\n"
			+"FROM airports A1\n"
			+"LEFT JOIN flights F1 ON F1.origin_airport_id= A1.airport_id AND date(F1.departure_dt)=? GROUP BY A1.airport_id),\n"
			+"arriveFlights AS(\n"
			+"SELECT airport_name, COUNT(F2.flights_id) as arriveCount\n"
			+"FROM airports A2\n"
			+"LEFT JOIN flights F2 ON F2.dest_airport_id= A2.airport_id AND date(F2.arrival_dt) =? GROUP BY A2.airport_id)\n"
			+"SELECT airport_name, departCount, arriveCount\n"
			+"FROM departFlights NATURAL JOIN arriveFlights\n"
			+"WHERE airport_name = ?;"
			);

/*
		PreparedStatement stat = conn.prepareStatement(
			"SELECT A.airport_name, COUNT(F.dest_airport_id), COUNT(F.origin_airport_id)\n"
			+"FROM flights F\n"
			+"LEFT JOIN airports A1 ON A1.airport_id = F.origin_airport_id AND date(F.departure_dt)=? \n"
			+"LEFT JOIN airports A2 ON A2.airport_id = F.dest_airport_id AND date(F.arrival_dt)=? \n"
			+"NATURAL JOIN airports A\n"
			+"WHERE A.airport_name = ?\n"
			+"GROUP BY A.airport_id\n"
			+"ORDER BY A.airport_name ASC;"
			);
*/
		stat.setString(1, final_date);
		stat.setString(2, final_date);
		stat.setString(3, args[3]);
		return stat.executeQuery();
	}

	public ResultSet query7(String[] args) throws SQLException
	{

		String start_date= args[2];
		String final_start_date = null;
		try{
			final_start_date = normalize_date_time(start_date);
		} catch(Exception e) {}

		String end_date= args[3];
		String final_end_date = null;
		try{
			final_end_date = normalize_date_time(end_date);
		} catch(Exception e) {}

		PreparedStatement stat = conn.prepareStatement (
			"WITH departFlights AS (\n"
			+"SELECT A.airline_name, F.flight_num, F.cancelled, F.depart_diff, F.arrival_diff\n"
			+"FROM airlines A\n"
			+"JOIN flights F ON F.airline_id= A.airline_id \n"
			+"WHERE A.airline_name=? AND F.flight_num=? AND date(departure_dt) BETWEEN ? AND ?),\n"
			+"departScheduled AS(\n"
			+"SELECT COUNT(*) FROM departFlights),\n"
			+"cancelledCount AS(\n"
			+"SELECT COUNT(*) FROM departFlights WHERE cancelled=1),\n"
			+"departedEarly AS(\n"
			+"SELECT COUNT(*) FROM departFlights WHERE cancelled=0 AND depart_diff <=0),\n"
			+"departedLate AS(\n"
			+"SELECT COUNT(*) FROM departFlights WHERE cancelled=0 AND depart_diff >0),\n"
			+"arrivedEarly AS(\n"
			+"SELECT COUNT(*) FROM departFlights WHERE cancelled=0 AND arrival_diff <=0),\n"
			+"arrivedLate AS(\n"
			+"SELECT COUNT(*) FROM departFlights WHERE cancelled=0 AND arrival_diff >0)\n"
			+"SELECT * FROM departScheduled, cancelledCount, departedEarly, departedLate, arrivedEarly, arrivedLate;"
			);

		stat.setString(1, args[0]);
		stat.setString(2, args[1]);
		stat.setString(3, final_start_date);
		stat.setString(4, final_end_date);
		return stat.executeQuery();
	}

		public ResultSet query8(String[] args) throws SQLException
	{

		String date = args[4];
		String final_date = null;
		try{
			final_date = normalize_date_time(date);
		} catch(Exception e) {}

			PreparedStatement stat = conn.prepareStatement (
				"SELECT A.airline_code, F3.flight_num, DA.airport_code as dest_code,\n"
				+"strftime('%H:%M',time(F3.departure_dt), depart_diff || ' minute'), AA.airport_code,\n"
				+"strftime('%H:%M', time(F3.arrival_dt), arrival_diff || ' minute'),\n"
				+"((strftime('%H', strftime('%H:%M',time(F3.arrival_dt), arrival_diff || ' minute'))-strftime('%H', strftime('%H:%M',time(F3.departure_dt), depart_diff || ' minute')))*60) + (strftime('%M', strftime('%H:%M',time(F3.arrival_dt), arrival_diff || ' minute'))-strftime('%M', strftime('%H:%M',time(F3.departure_dt), depart_diff || ' minute'))) AS duration \n"
				+"FROM airlines A\n"
				+"JOIN flights F3 ON F3.airline_id = A.airline_id\n"
				+"JOIN airports DA ON DA.airport_id = F3.origin_airport_id\n"
				+"JOIN airports AA ON AA.airport_id = F3.dest_airport_id\n"
				+"WHERE F3.cancelled=0 AND DA.city = ? AND DA.state = ? AND AA.city = ? AND AA.state = ? AND date(departure_dt) = ? AND date(arrival_dt) = ?\n"
				+"ORDER BY duration ASC,  airline_code ASC;"
				);

		stat.setString(1, args[0]);
		stat.setString(2, args[1]);
		stat.setString(3, args[2]);
		stat.setString(4, args[3]);
		stat.setString(5, final_date);
		stat.setString(6, final_date);

		return stat.executeQuery();
	}

	public ResultSet query9(String[] args) throws SQLException
{

	String date = args[4];
	String final_date = null;
	try{
		final_date = normalize_date_time(date);
	} catch(Exception e) {}

	PreparedStatement stat = conn.prepareStatement (
			"WITH airlinesData AS (SELECT airline_code, flight_num, DA.airport_code as depart_code, DA.city AS depart_city, DA.state AS depart_state,\n"
			+"strftime('%Y-%m-%d %H:%M',departure_dt, depart_diff || ' minute') AS depart_time, AA.airport_code AS arrive_code, AA.city AS arrive_city, AA.state AS arrive_state,\n"
			+"strftime('%Y-%m-%d %H:%M', arrival_dt, arrival_diff || ' minute') AS arrive_time\n"
			+"FROM flights F3\n"
			+"JOIN airlines A ON F3.airline_id = A.airline_id\n"
			+"JOIN airports DA ON DA.airport_id = F3.origin_airport_id\n"
			+"JOIN airports AA ON AA.airport_id = F3.dest_airport_id\n"
			+"WHERE F3.cancelled=0 AND date(depart_time) = ? AND date(arrive_time) = ?)\n"
			+"SELECT A1.airline_code, A1.flight_num, A1.depart_code, \n"
			+"strftime('%H:%M', A1.depart_time) AS depart_time1, A1.arrive_code, strftime('%H:%M', A1.arrive_time), \n"
			+"A2.airline_code, A2.flight_num, A2.depart_code, strftime('%H:%M', A2.depart_time), A2.arrive_code, strftime('%H:%M', A2.arrive_time),\n"
			+"(strftime('%H',A2.arrive_time) - strftime('%H',A1.depart_time))*60 + strftime('%M',A2.arrive_time) - strftime('%M',A1.depart_time) AS duration\n"
			+"FROM airlinesData A1\n"
			+"JOIN airlinesData A2 ON A1.arrive_code = A2.depart_code\n"
			+"WHERE A1.depart_city = ? AND A1.depart_state = ? AND A2.arrive_city = ? AND A2.arrive_state=?\n"
			+"AND (A1.depart_city != A2.depart_city OR A1.depart_state != A2.depart_state)\n"
			+"AND (A1.arrive_city != A2.arrive_city OR A1.arrive_state != A2.arrive_state)\n"
			+"AND A1.arrive_time < A2.depart_time\n"
			+"ORDER BY duration, A1.airline_code, A2.airline_code, depart_time1;"
			);

	stat.setString(1, final_date);
	stat.setString(2, final_date);
	stat.setString(3, args[0]);
	stat.setString(4, args[1]);
	stat.setString(5, args[2]);
	stat.setString(6, args[3]);


	return stat.executeQuery();
}

public ResultSet query10(String[] args) throws SQLException
{

String date = args[4];
String final_date = null;
try{
	final_date = normalize_date_time(date);
} catch(Exception e) {}


	PreparedStatement stat = conn.prepareStatement (
			"WITH airlinesData AS (SELECT airline_code, flight_num, DA.airport_code as depart_code, DA.city AS depart_city, DA.state AS depart_state,\n"
			+"strftime('%Y-%m-%d %H:%M',departure_dt, depart_diff || ' minute') AS depart_time, AA.airport_code AS arrive_code, AA.city AS arrive_city, AA.state AS arrive_state,\n"
			+"strftime('%Y-%m-%d %H:%M', arrival_dt, arrival_diff || ' minute') AS arrive_time\n"
			+"FROM flights F3\n"
			+"JOIN airlines A ON F3.airline_id = A.airline_id\n"
			+"JOIN airports DA ON DA.airport_id = F3.origin_airport_id\n"
			+"JOIN airports AA ON AA.airport_id = F3.dest_airport_id\n"
			+"WHERE F3.cancelled=0 AND date(depart_time) = ? AND date(arrive_time) = ?)\n"
			+"SELECT A1.airline_code, A1.flight_num, A1.depart_code, \n"
			+"strftime('%H:%M', A1.depart_time) AS depart_time1, A1.arrive_code, strftime('%H:%M', A1.arrive_time), \n"
			+"A2.airline_code, A2.flight_num, A2.depart_code, strftime('%H:%M', A2.depart_time), A2.arrive_code, strftime('%H:%M', A2.arrive_time),\n"
			+"A3.airline_code, A3.flight_num, A3.depart_code, \n"
			+"strftime('%H:%M', A3.depart_time), A3.arrive_code, strftime('%H:%M', A3.arrive_time),\n"
			+"(strftime('%H',A3.arrive_time) - strftime('%H',A1.depart_time))*60 + strftime('%M',A3.arrive_time) - strftime('%M',A1.depart_time) AS duration\n"
			+"FROM airlinesData A1\n"
			+"JOIN airlinesData A2 ON A1.arrive_code = A2.depart_code\n"
			+"JOIN airlinesData A3 ON A2.arrive_code = A3.depart_code\n"
			+"WHERE A1.depart_city = ? AND A1.depart_state = ? AND A3.arrive_city = ? AND A3.arrive_state=?\n"
			+"AND (A3.depart_city != A1.depart_city OR A3.depart_state != A1.depart_state)\n"
			+"AND (A3.arrive_city != A1.arrive_city OR A3.arrive_state != A1.arrive_state)\n"
			+"AND A1.arrive_time < A2.depart_time AND A2.arrive_time < A3.depart_time\n"
			+"ORDER BY duration, A1.airline_code, A2.airline_code, A3.airline_code, depart_time1;"
			);

stat.setString(1, final_date);
stat.setString(2, final_date);
stat.setString(3, args[0]);
stat.setString(4, args[1]);
stat.setString(5, args[2]);
stat.setString(6, args[3]);



return stat.executeQuery();
}

	public String normalize_date_time(String dateString) throws Exception
	{

		DateFormat standardDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		DateFormat sampleDateFormat = new SimpleDateFormat("MM/dd/yyyy");

		Date dateParsed = new Date();
		String date = null;

		try {
			sampleDateFormat.setLenient(false);
			dateParsed = sampleDateFormat.parse(dateString);
			date = standardDateFormat.format(dateParsed);
		} catch (Exception e) {
		}

		return date;
	}
}
