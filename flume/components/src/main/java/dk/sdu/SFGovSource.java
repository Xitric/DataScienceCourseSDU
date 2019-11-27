package dk.sdu;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.ExecSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
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
		if(now.isAfter(nextRun)) {
			nextRun = nextRun.plusDays(1);
		}

		long delay = Duration.between(now, nextRun).getSeconds();

		executor = Executors.newScheduledThreadPool(1);
		//TODO: Use this when done testing
//		executor.scheduleAtFixedRate(new SFGovSourceRunner(batchSize, getChannelProcessor()),
//				delay,
//				TimeUnit.HOURS.toSeconds(scheduledRecurrence),
//				TimeUnit.SECONDS);

		executor.scheduleAtFixedRate(new SFGovSourceRunner(batchSize, getChannelProcessor()),
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
		private final ChannelProcessor channelProcessor;
		private final List<Event> bufferedEvents;

		public SFGovSourceRunner(int batchSize, ChannelProcessor channelProcessor) {
			this.batchSize = batchSize;
			this.channelProcessor = channelProcessor;
			this.bufferedEvents = new ArrayList<>();
		}

		@Override
		public void run() {
			logger.info("SFGovSource started");

			//TODO: Read from API
			for (int i = 0; i < 10; i++) {
				bufferedEvents.add(EventBuilder.withBody("Hello, world " + i, Charset.defaultCharset()));
				if (bufferedEvents.size() >= batchSize) {
					processBatch();
				}
			}

			if (!bufferedEvents.isEmpty()) {
				processBatch();
			}
		}

		private void processBatch() {
			channelProcessor.processEventBatch(bufferedEvents);
			bufferedEvents.clear();
			logger.info("SFGovSource processing batch");
		}
	}
}
