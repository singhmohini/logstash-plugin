package jenkins.plugins.logstash.persistence;

import com.github.wnameless.json.flattener.JsonFlattener;
import com.google.gson.JsonObject;
import io.logz.sender.FormattedLogMessage;
import io.logz.sender.HttpsRequestConfiguration;
import io.logz.sender.HttpsSyncSender;
import io.logz.sender.SenderStatusReporter;
import io.logz.sender.exceptions.LogzioParameterErrorException;
import io.logz.sender.exceptions.LogzioServerErrorException;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Logz.io Data Access Object.
 *
 * @author Ido Halevi
 *
 */

public class LogzioDao extends AbstractLogstashIndexerDao {
    private final String TYPE = "jenkins_logstash_plugin";
    private String key;
    private String host;
    private LogzioHttpsClient httpsClient;

    //primary constructor used by indexer factory
    public LogzioDao(String host, String key){
        this(null, host, key);
    }

    // Factored for unit testing
    LogzioDao(LogzioHttpsClient factory, String host, String key) {
        this.host = host;
        this.key = key;
        try{
            this.httpsClient = factory == null ? new LogzioHttpsClient(key, host, TYPE) : factory;
        }catch (LogzioParameterErrorException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void push(String data) throws IOException {
        JSONObject jsonData = JSONObject.fromObject(data);
        JSONArray logMessages = jsonData.getJSONArray("message");
        if (!logMessages.isEmpty()) {
            try{
                for (Object logMsg : logMessages) {
                    JsonObject logLine = createLogLine(jsonData, logMsg.toString());
                    httpsClient.send(new FormattedLogMessage((logLine + "\n").getBytes(Charset.forName("UTF-8"))));
                }
                httpsClient.flush();
            }catch (LogzioServerErrorException e){
                throw new IOException(e);
            }
        }
    }


    private JsonObject createLogLine(JSONObject jsonData, String logMsg) {
        JsonObject logLine = new JsonObject();

        logLine.addProperty("message", logMsg);
        logLine.addProperty("@timestamp", ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));

        for (Object key : jsonData.keySet()) {
            if (!key.equals("message")){
                logLine.addProperty(key.toString(), jsonData.getString(key.toString()));
            }
        }

        return logLine;
    }

    @Override
    public JSONObject buildPayload(BuildData buildData, String jenkinsUrl, List<String> logLines) {
        JSONObject payload = new JSONObject();
        payload.put("message", logLines);
        payload.put("source", "jenkins");
        payload.put("source_host", jenkinsUrl);
        payload.put("@buildTimestamp", buildData.getTimestamp());
        payload.put("@version", 1);
        // Flatten build data - this is for the user to be able to use this fields for visualization in Kibana.
        // In addition, it makes the query much easier.
        Map<String, Object> flattenJson = JsonFlattener.flattenAsMap(buildData.toString());
        for (Map.Entry<String, Object> entry : flattenJson.entrySet()) {
            String key = entry.getKey().replace('.','_');
            Object value = entry.getValue();
            payload.put(key, value);
        }

        return payload;
    }

    @Override
    public String getDescription(){ return host; }

    public String getHost(){ return host; }

    public String getKey(){ return key; }

    public String getType(){ return TYPE; }

    public static class LogzioHttpsClient{
        private static final int MAX_SIZE_IN_BYTES = 8 * 1024 * 1024;  // 8 MB
        private static final int CONNECT_TIMEOUT = 10 * 1000;
        private static final int SOCKET_TIMEOUT = 10 * 1000;
        private final HttpsSyncSender logzioClient;
        private List<FormattedLogMessage> messages;
        private int size;
        private final SenderStatusReporter reporter;

        LogzioHttpsClient(String token, String listener, String type) throws LogzioParameterErrorException {
            HttpsRequestConfiguration gzipHttpsRequestConfiguration = HttpsRequestConfiguration
                    .builder()
                    .setLogzioToken(token)
                    .setLogzioType(type)
                    .setLogzioListenerUrl(listener)
                    .setSocketTimeout(SOCKET_TIMEOUT)
                    .setConnectTimeout(CONNECT_TIMEOUT)
                    .setCompressRequests(true)
                    .build();
            reporter = new Reporter();
            logzioClient = new HttpsSyncSender(gzipHttpsRequestConfiguration, reporter);
            messages = new ArrayList<>();
            size = 0;
        }

        public void send(FormattedLogMessage log) throws LogzioServerErrorException {
            messages.add(log);
            size += log.getSize();
            if (size > MAX_SIZE_IN_BYTES) {
                sendAndReset();
            }
        }

        private void reset(){
            size = 0;
            messages.clear();
        }

        void flush() throws LogzioServerErrorException {
            if(messages.size() > 0 ) {
                sendAndReset();
            }
        }

        private void sendAndReset() throws LogzioServerErrorException {
            logzioClient.sendToLogzio(messages);
            reset();
        }

        private static class Reporter implements SenderStatusReporter{
            private static final Logger LOGGER = Logger.getLogger(LogzioDao.class.getName());

            private void pringLogMessage(Level level, String msg) {
                LOGGER.log(level, msg);
            }

            @Override
            public void error(String msg) {
                pringLogMessage(Level.SEVERE, "[LogzioSender]ERROR: " + msg);
            }

            @Override
            public void error(String msg, Throwable e) {
                pringLogMessage(Level.SEVERE, "[LogzioSender]ERROR: " + msg + "\n" +e);
            }

            @Override
            public void warning(String msg) {
                pringLogMessage(Level.WARNING, "[LogzioSender]WARNING: " + msg);
            }

            @Override
            public void warning(String msg, Throwable e) {
                pringLogMessage(Level.WARNING, "[LogzioSender]WARNING: " + msg + "\n" + e);
            }

            @Override
            public void info(String msg) {
                pringLogMessage(Level.INFO, "[LogzioSender]INFO: " + msg);
            }

            @Override
            public void info(String msg, Throwable e) {
                pringLogMessage(Level.INFO, "[LogzioSender]INFO: " + msg + "\n" + e);
            }
        }
    }
}
