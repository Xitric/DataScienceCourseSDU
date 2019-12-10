package dk.sdu;

/**
 * Flume configuration constants.
 *
 * @author Kasper
 */
public interface SFGovSourceConfigurationConstants {

	String CONFIG_BATCH_SIZE = "batchSize";
	int DEFAULT_BATCH_SIZE = 20;
	String CONFIG_SCHEDULED_TIME_ZONE = "scheduledTimeZone";
	String DEFAULT_SCHEDULED_TIME_ZONE = "Z";
	String CONFIG_SCHEDULED_HOUR = "scheduledHour";
	int DEFAULT_SCHEDULED_HOUR = 0;
	String CONFIG_SCHEDULED_RECURRENCE = "scheduledRecurrence";
	int DEFAULT_SCHEDULED_RECURRENCE = 24;
	String CONFIG_DATA_SOURCE = "dataSource";
	String DEFAULT_DATA_SOURCE = "";
	String CONFIG_TIME_FIELD = "timeField";
	String DEFAULT_TIME_FIELD = "";
}
