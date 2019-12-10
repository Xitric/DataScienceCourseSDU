package dk.sdu;

import java.sql.*;
import java.time.LocalDateTime;

/**
 * Client for contacting the MySQL database and getting information about the latest messages, that have been ingested.
 *
 * @author Kasper
 */
public class MySqlClient {

	private final String user = System.getenv("SFGOV_MYSQL_USER");
	private final String password = System.getenv("SFGOV_MYSQL_PASSWORD");
	private final String server = System.getenv("SFGOV_MYSQL_SERVER");
	private final String dataSource;

	public MySqlClient(String dataSource) {
		this.dataSource = dataSource;
	}

	public LocalDateTime getLatestTime() {
		ResultSet result = null;

		try (Connection connection = getConnection();
		     PreparedStatement statement = connection.prepareStatement("SELECT latest FROM data_ingestion_latest WHERE data_source = ?")) {
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

	public boolean isLatestEntryMissing() {
		ResultSet result = null;

		try (Connection connection = getConnection();
		     PreparedStatement statement = connection.prepareStatement("SELECT COUNT(*) FROM data_ingestion_latest WHERE data_source = ?")) {
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
}
