package dk.sdu;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.ExecSource;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
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

	private static final Logger logger = LoggerFactory.getLogger(ExecSource.class);

	//Config
	private int batchSize;
	private ZoneId scheduledTimeZone;
	private int scheduledHour;
	private int scheduledRecurrence;
	private String appToken;

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

		appToken = context.getString(SFGovSourceConfigurationConstants.CONFIG_APP_TOKEN,
				SFGovSourceConfigurationConstants.DEFAULT_APP_TOKEN);
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

		executor.scheduleAtFixedRate(new SFGovSourceRunner(batchSize, appToken, getChannelProcessor()),
				0,
				20,
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
		private final String appToken;
		private final ChannelProcessor channelProcessor;
		private final List<Event> bufferedEvents;
		private final CloseableHttpClient httpClient;
		private LocalDateTime latestTime;
		private MySqlClient hdfsClient;

		public SFGovSourceRunner(int batchSize, String appToken, ChannelProcessor channelProcessor) {
			this.batchSize = batchSize;
			this.appToken = appToken;
			this.channelProcessor = channelProcessor;
			this.bufferedEvents = new ArrayList<>();
			this.httpClient = HttpClients.createDefault();
			this.hdfsClient = new MySqlClient();
		}

		@Override
		public void run() {
			//TODO: Read latest from DB
			latestTime = hdfsClient.getLatestTime();
			if (latestTime == null) {
				logger.info("Error reading latest service case time from MySql, no data will be fetched");
				if (hdfsClient.isLatestEntryMissing()) {
					logger.info("Entry with latest service case time is missing in MySql");
//					hdfsClient.saveLatestTime(LocalDateTime.now());
				}
				return;
			}

			logger.info("SFGovSource started");
			//TODO: Iterate until no more data, do-while?
			JsonArray results = getSince(latestTime);

			for (JsonElement result : results) {
				updateLatest(result.getAsJsonObject());
				bufferedEvents.add(EventBuilder.withBody(result.toString(), Charset.defaultCharset()));
				if (bufferedEvents.size() >= batchSize) {
					processBatch();
				}
			}
			//			for (int i = 0; i < 10; i++) {
			//				bufferedEvents.add(EventBuilder.withBody("{\"incident_datetime\":\"2019-08-15T11:41:00.000\",\"incident_date\":\"2019-08-15T00:00:00.000\",\"incident_time\":\"11:41\",\"incident_year\":\"2019\",\"incident_day_of_week\":\"Thursday\",\"report_datetime\":\"2019-10-01T14:06:00.000\",\"row_id\":\"85424006374\",\"incident_id\":\"854240\",\"incident_number\":\"196208089\",\"report_type_code\":\"II\",\"report_type_description\":\"Coplogic Initial\",\"filed_online\": true,\"incident_code\":\"06374\",\"incident_category\":\"Larceny Theft\",\"incident_subcategory\":\"Larceny Theft - Other\",\"incident_description\":\"Theft, Other Property, >$950\",\"resolution\":\"Open or Active\",\"police_district\":\"Central\"}", Charset.defaultCharset()));
			//				if (bufferedEvents.size() >= batchSize) {
			//					processBatch();
			//				}mo
			//			}

			if (!bufferedEvents.isEmpty()) {
				processBatch();
			}

			//TODO: Persist latest in DB
		}

		private JsonArray getSince(LocalDateTime last) {
			try {
				URI uri = new URIBuilder("https://data.sfgov.org/resource/vw6y-z8j6.json") //TODO: Inject uri
						.setParameter("$limit", "10")
						.build();
				HttpGet request = new HttpGet(uri);
				request.addHeader("X-App-Token", appToken);

				try (CloseableHttpResponse response = httpClient.execute(request)) {
					if (response.getStatusLine().getStatusCode() == 200) {
						JsonElement responseBody = new JsonParser().parse(EntityUtils.toString(response.getEntity()));

						if (responseBody.isJsonArray()) {
							return responseBody.getAsJsonArray();
						}
					}
				}
			} catch (URISyntaxException | IOException e) {
				e.printStackTrace();
			}

			return new JsonArray();
		}

		private void updateLatest(JsonObject potentialLatest) {
			String openedString = potentialLatest.getAsJsonPrimitive("requested_datetime").getAsString();
			LocalDateTime openedTime = TimeUtil.toDateTime(openedString);

			if (openedTime.isAfter(latestTime)) {
				latestTime = openedTime;
			}
		}

		private void processBatch() {
			channelProcessor.processEventBatch(bufferedEvents);
			bufferedEvents.clear();
			logger.info("SFGovSource processing batch");
		}
	}
}
