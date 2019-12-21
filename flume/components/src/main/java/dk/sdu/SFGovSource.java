package dk.sdu;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Flume source for reading newly published data from the SFGov API. Can be configured to run at a certain time of the
 * day, since the data is updated every 24 hours.
 *
 * @author Kasper
 */
public class SFGovSource extends AbstractSource implements EventDrivenSource, Configurable {

	private static final Logger logger = LoggerFactory.getLogger(SFGovSource.class);
	private static final int RETRY_COUNT = 10;
	private static final String DATA_SOURCE_HEADER = "Data-Source";

	//Config
	private int batchSize;
	private ZoneId scheduledTimeZone;
	private int scheduledHour;
	private int scheduledRecurrence;
	private String dataSource;
	private String timeField;

	//Runner
	private ScheduledExecutorService executor;

	@Override
	public void configure(Context context) {
		batchSize = context.getInteger(SFGovSourceConfigurationConstants.CONFIG_BATCH_SIZE,
				SFGovSourceConfigurationConstants.DEFAULT_BATCH_SIZE);

		String scheduledTimeZoneString = context.getString(SFGovSourceConfigurationConstants.CONFIG_SCHEDULED_TIME_ZONE,
				SFGovSourceConfigurationConstants.DEFAULT_SCHEDULED_TIME_ZONE);
		scheduledTimeZone = ZoneId.of(scheduledTimeZoneString);

		scheduledHour = context.getInteger(SFGovSourceConfigurationConstants.CONFIG_SCHEDULED_HOUR,
				SFGovSourceConfigurationConstants.DEFAULT_SCHEDULED_HOUR);

		scheduledRecurrence = context.getInteger(SFGovSourceConfigurationConstants.CONFIG_SCHEDULED_RECURRENCE,
				SFGovSourceConfigurationConstants.DEFAULT_SCHEDULED_RECURRENCE);

		dataSource = context.getString(SFGovSourceConfigurationConstants.CONFIG_DATA_SOURCE,
				SFGovSourceConfigurationConstants.DEFAULT_DATA_SOURCE);

		timeField = context.getString(SFGovSourceConfigurationConstants.CONFIG_TIME_FIELD,
				SFGovSourceConfigurationConstants.DEFAULT_TIME_FIELD);
	}

	@Override
	public void start() {
		ZonedDateTime now = ZonedDateTime.now(scheduledTimeZone);
		ZonedDateTime nextRun = now.withHour(scheduledHour).withMinute(0).withSecond(0);
		if (now.isAfter(nextRun)) {
			nextRun = nextRun.plusDays(1);
		}

		long delay = Duration.between(now, nextRun).getSeconds();

		executor = Executors.newScheduledThreadPool(1);
		//TODO: Use this when done testing
		//		executor.scheduleAtFixedRate(new SFGovSourceRunner(batchSize, appToken, getChannelProcessor()),
		//				delay,
		//				TimeUnit.HOURS.toSeconds(scheduledRecurrence),
		//				TimeUnit.SECONDS);

		executor.scheduleAtFixedRate(new SFGovSourceRunner(batchSize, dataSource, timeField, scheduledTimeZone, getChannelProcessor()),
				0,
				TimeUnit.HOURS.toSeconds(scheduledRecurrence),
				TimeUnit.SECONDS);
		logger.info("SFGovSource scheduled for running");

		super.start();
	}

	@Override
	public void stop() {
		logger.info("Shutting down SFGovSource...");
		executor.shutdownNow();

		while (!executor.isTerminated()) {
			try {
				executor.awaitTermination(1000, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		logger.info("SFGovSource stopped");
		super.stop();
	}

	private static final class SFGovSourceRunner implements Runnable {

		private final int batchSize;
		private final String timeField;
		private ZoneId scheduledTimeZone;
		private final ChannelProcessor channelProcessor;
		private final Queue<EventWrapper> bufferedEvents;
		private final Map<String, String> headers;
		private final MySqlClient mysqlClient;
		private final SfGovClient httpClient;
		private LocalDateTime latestTime;

		public SFGovSourceRunner(int batchSize, String dataSource, String timeField, ZoneId scheduledTimeZone, ChannelProcessor channelProcessor) {
			this.batchSize = batchSize;
			this.timeField = timeField;
			this.scheduledTimeZone = scheduledTimeZone;
			this.channelProcessor = channelProcessor;

			this.bufferedEvents = new PriorityQueue<>();
			this.headers = new HashMap<>();
			this.headers.put(DATA_SOURCE_HEADER, dataSource);

			this.mysqlClient = new MySqlClient(dataSource);
			this.httpClient = new SfGovClient(dataSource, timeField);
		}

		@Override
		public void run() {
			latestTime = retryGetLatest();
			if (latestTime == null) {
				logger.info("Error reading latest service case time from MySql, no data will be fetched");
				return;
			}

			logger.info("SFGovSource started");

			int count = 0;
			JsonArray results;
			while ((results = httpClient.getSince(latestTime)).size() > 0) {
				for (JsonElement result : results) {
					count++;

					String timeString = result.getAsJsonObject().getAsJsonPrimitive(timeField).getAsString();
					updateLatest(timeString);

					Map<String, String> eventHeaders = new HashMap<>(headers);
					eventHeaders.put("time", timeString);

					bufferedEvents.add(new EventWrapper(EventBuilder.withBody(result.toString(), Charset.defaultCharset(), eventHeaders)));
				}
			}

			logger.info("SFGovSource done reading new data (" + count + ")");

			broadcast();
		}

		private LocalDateTime retryGetLatest() {
			int retries = RETRY_COUNT;
			LocalDateTime latest;
			while ((latest = mysqlClient.getLatestTime()) == null) {
				if (mysqlClient.isLatestEntryMissing()) {
					logger.info("Entry with latest service case time is missing in MySql");
					break;
				}

				retries--;
				if (retries == 0) {
					logger.info("Failed to connect to MySql in " + RETRY_COUNT + " attempts");
					break;
				}
			}

			return latest;
		}

		private void updateLatest(String openedString) {
			LocalDateTime openedTime = TimeUtil.toDateTime(openedString);

			if (openedTime.isAfter(latestTime)) {
				latestTime = openedTime;
			}
		}

		private void broadcast() {
			while (! bufferedEvents.isEmpty()) {
				Event event = bufferedEvents.poll().event;

				//Schedule event 24 hours late 
				DateTime eventTime = DateTime.parse(event.getHeaders().get("time")).plusDays(1); 

				ZonedDateTime now = ZonedDateTime.now(scheduledTimeZone);
				ZonedDateTime nextRun = ZonedDateTime.now()
						.withHour(eventTime.getHourOfDay())
						.withMinute(eventTime.getMinuteOfHour())
						.withSecond(eventTime.getSecondOfMinute());

				long delay = Duration.between(now, nextRun).getSeconds() * 1000;

				if (delay > 0) {
					try {
						logger.info("SFGovSource scheduled event to " + nextRun.toString());
						Thread.sleep(delay);
					} catch (InterruptedException e) {
						return;
					}
				} else if (delay < 0) { 
					//TODO: Handle this better 
					continue; 
				} 
				channelProcessor.processEvent(event);
				logger.info("SFGovSource sending event at time " + nextRun.toString());
			}
		}
	}

	private static class EventWrapper implements Comparable<EventWrapper> {

		private final Event event;

		public EventWrapper(Event event) {
			this.event = event;
		}

		@Override
		public int compareTo(EventWrapper other) {
			DateTime thisTime = DateTime.parse(this.event.getHeaders().get("time"));
			DateTime otherTime = DateTime.parse(other.event.getHeaders().get("time"));

			ZonedDateTime thisCorrectedTime = ZonedDateTime.now()
					.withHour(thisTime.getHourOfDay())
					.withMinute(thisTime.getMinuteOfHour())
					.withSecond(thisTime.getSecondOfMinute());
			ZonedDateTime otherCorrectedTime = ZonedDateTime.now()
					.withHour(otherTime.getHourOfDay())
					.withMinute(otherTime.getMinuteOfHour())
					.withSecond(otherTime.getSecondOfMinute());

			return thisCorrectedTime.compareTo(otherCorrectedTime);
		}
	}
}
