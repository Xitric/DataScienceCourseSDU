package dk.sdu;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Utility for parsing time strings.
 *
 * @author Kasper
 */
public class TimeUtil {

	public static LocalDateTime toDateTime(String time) {
		return LocalDateTime.parse(time, DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS"));
	}
}
