package dk.sdu;

import java.sql.*;
import java.time.LocalDateTime;

/**
 * @author Kasper
 */
public class MySqlClient {

	//TODO: Inject config
	private final String user = "client";
	private final String password = "H8IAQzX236eu5Ep0";
	private final String server = "jdbc:mysql://mysql:3306/flume";
	private final String dataSource = "vw6y-z8j6";

	public MySqlClient() {
	}

	public LocalDateTime getLatestTime() {
		ResultSet result = null;

		try (Connection connection = getConnection();
		     PreparedStatement statement = connection.prepareStatement("SELECT latest FROM data_ingestion_latest WHERE data_source = ?")) {
			//TODO: Inject config
			statement.setString(1, dataSource);
			result = statement.executeQuery();

			if (result.first()) {
				return result.getTimestamp(1).toLocalDateTime();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (result != null) {
					result.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	//TODO: Do not use this
	public void saveLatestTime(LocalDateTime time) {
		try (Connection connection = getConnection();
		     PreparedStatement statement = connection.prepareStatement("INSERT INTO data_ingestion_latest (data_source, latest) VALUES (?, ?) ON DUPLICATE KEY UPDATE latest = ?")) {
			//TODO: Inject config
			statement.setString(1, dataSource);
			statement.setTimestamp(2, Timestamp.valueOf(time));
			statement.setTimestamp(3, Timestamp.valueOf(time));
			statement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public boolean isLatestEntryMissing() {
		ResultSet result = null;

		try (Connection connection = getConnection();
		     PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM data_ingestion_latest WHERE data_source = ?")) {
			//TODO: Inject config
			statement.setString(1, dataSource);
			result = statement.executeQuery();

			if (result.first()) {
				return result.getInt(1) == 0;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (result != null) {
					result.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		return true;
	}
	private Connection getConnection() throws SQLException {
		return DriverManager.getConnection(server, user, password);
	}

	public static void main(String[] args) {
		MySqlClient c = new MySqlClient();
		c.isLatestEntryMissing();
	}
}
