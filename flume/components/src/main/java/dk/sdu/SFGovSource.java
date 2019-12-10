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
	private static final int RETRY_COUNT = 10;

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
				60,
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
			latestTime = retryGetLatest();
			if (latestTime == null) {
				logger.info("Error reading latest service case time from MySql, no data will be fetched");
				return;
			}

			logger.info("SFGovSource started");

			JsonArray results;
			while ((results = getSince(latestTime)).size() > 0) {
				for (JsonElement result : results) {
					updateLatest(result.getAsJsonObject());
					bufferedEvents.add(EventBuilder.withBody(result.toString(), Charset.defaultCharset()));
					if (bufferedEvents.size() >= batchSize) {
						processBatch();
					}
				}
			}

			if (!bufferedEvents.isEmpty()) {
				processBatch();
			}

			logger.info("SFGovSource done reading new data");
		}

		private LocalDateTime retryGetLatest() {
			int retries = RETRY_COUNT;
			LocalDateTime latest;
			while ((latest = hdfsClient.getLatestTime()) == null) {
				if (hdfsClient.isLatestEntryMissing()) {
					logger.info("Entry with latest service case time is missing in MySql");
					break;
				}

				retries --;
				if (retries == 0) {
					logger.info("Failed to connect to MySql in " + RETRY_COUNT + " attempts");
					break;
				}
			}

			return latest;
		}

		private JsonArray getSince(LocalDateTime last) {
			try {
				URI uri = new URIBuilder("https://data.sfgov.org/resource/vw6y-z8j6.json") //TODO: Inject uri
						.setParameter("$limit", "10") //TODO: Remove when done testing
						.setParameter("$where", "requested_datetime>'" + last.toString() + "'")
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
