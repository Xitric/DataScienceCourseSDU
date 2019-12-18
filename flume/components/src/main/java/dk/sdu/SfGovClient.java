package dk.sdu;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.http.HttpStatus;
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
import java.time.LocalDateTime;

/**
 * Client for contacting the SFGov API to retrieve messages after a given date.
 *
 * @author Kasper
 */
public class SfGovClient {

	private static final Logger logger = LoggerFactory.getLogger(SfGovClient.class);
	private static final String WHERE = "$where";
	private static final String TIME_QUERY = "%s>'%s'";
	private static final String APP_TOKEN_HEADER = "X-App-Token";

	private final String baseURI = System.getenv("SFGOV_BASE_URI");
	private final String appToken = System.getenv("SFGOV_APP_TOKEN");
	private final String dataSource;
	private final String timeField;
	private final CloseableHttpClient httpClient;

	public SfGovClient(String dataSource, String timeField) {
		this.dataSource = dataSource;
		this.timeField = timeField;
		this.httpClient = HttpClients.createDefault();
	}

	public JsonArray getSince(LocalDateTime last) {
		try {
			URI uri = new URIBuilder(String.format(baseURI, dataSource))
					.setParameter(WHERE, String.format(TIME_QUERY, timeField, last.toString()))
					.build();
			HttpGet request = new HttpGet(uri);
			request.addHeader(APP_TOKEN_HEADER, appToken);

			try (CloseableHttpResponse response = httpClient.execute(request)) {
				if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
					JsonElement responseBody = new JsonParser().parse(EntityUtils.toString(response.getEntity()));

					if (responseBody.isJsonArray()) {
						return responseBody.getAsJsonArray();
					}
				}
			}
		} catch (URISyntaxException | IOException e) {
			logger.error("Error contacting API", e);
		}

		return new JsonArray();
	}
}
