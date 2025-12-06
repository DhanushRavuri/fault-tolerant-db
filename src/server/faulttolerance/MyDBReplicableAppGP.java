package server.faulttolerance;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import org.json.JSONObject;

public class MyDBReplicableAppGP implements Replicable {

  private static final Logger logger = Logger.getLogger(MyDBReplicableAppGP.class.getName());

  public static final int SLEEP = 1000;
  public static final int MAX_LOG_SIZE = 400;

  private Session session;
  private Cluster cluster;
  private final String keyspace;

  private final Map<String, Boolean> executedRequests;
  private final List<String> requestLog;

  private long lastCheckpointTime;
  private static final long CHECKPOINT_INTERVAL = 5000;

  private long latestExecutedSeq = 0;

  public MyDBReplicableAppGP(String[] args) {
    System.out.println("MyDBReplicableAppGP CONSTRUCTOR");

    if (args != null && args.length > 0) {

      String arg = args[0];
      if (arg.contains(":")) {
        String[] parts = arg.split(":");
        this.keyspace = parts.length >= 2 ? parts[1] : arg;
      } else {
        this.keyspace = arg;
      }
    } else {
      this.keyspace = "default";
    }

    System.out.println("Using keyspace: " + this.keyspace);

    this.executedRequests = new ConcurrentHashMap<>();
    this.requestLog = new ArrayList<>();
    initializeDatabase();
  }

  private void initializeDatabase() {
    try {
      this.cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9042).build();

      this.session = cluster.connect();

      createKeyspaceIfNotExists();

      session.execute("USE \"" + keyspace + "\";");

      createTableIfNotExists();

      System.out.println("Database initialized for keyspace: " + keyspace);

    } catch (Exception e) {
      System.err.println("Database initialization failed: " + e.getMessage());
      e.printStackTrace();
    }
  }

  private void createKeyspaceIfNotExists() {
    try {
      String createKeyspaceSQL =
          "CREATE KEYSPACE IF NOT EXISTS \""
              + keyspace
              + "\" "
              + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};";
      session.execute(createKeyspaceSQL);
    } catch (Exception e) {
      System.err.println("Could not create keyspace: " + keyspace);
    }
  }

  private void createTableIfNotExists() {
    try {
      String createTableSQL =
          "CREATE TABLE IF NOT EXISTS grade (" + "id int PRIMARY KEY, " + "events list<int>)";
      session.execute(createTableSQL);
    } catch (Exception e) {
      System.err.println("Could not create table in keyspace: " + keyspace);
    }
  }

  @Override
  public boolean execute(Request request, boolean doNotReply) {
    return execute(request);
  }

  @Override
  public boolean execute(Request request) {
    if (!(request instanceof RequestPacket)) {
      System.err.println("Not a RequestPacket: " + request.getClass());
      return false;
    }
    RequestPacket packet = (RequestPacket) request;

    String requestId = String.valueOf(packet.getRequestID());

    if (executedRequests.containsKey(requestId)) {
      System.out.println("Skipping duplicate request: " + requestId);
      return true;
    }

    String sql = extractSQLFromPacket(packet);
    if (sql == null || sql.isEmpty()) {
      System.err.println("No SQL to execute!");
      return false;
    }

    boolean success = executeSQL(sql);

    if (success) {
      executedRequests.put(requestId, true);

      synchronized (requestLog) {
        requestLog.add(requestId);
        if (requestLog.size() > MAX_LOG_SIZE) {
          String oldestId = requestLog.remove(0);
          executedRequests.remove(oldestId);
        }
      }

      updateLatestSequence(sql);

      if (System.currentTimeMillis() - lastCheckpointTime > CHECKPOINT_INTERVAL) {
        lastCheckpointTime = System.currentTimeMillis();
      }

      System.out.println("Executed request: " + requestId);
    }

    return success;
  }

  private String extractSQLFromPacket(RequestPacket packet) {
    try {
      String packetStr = packet.toString();

      if (packetStr.contains("\"QV\":")) {
        JSONObject json = new JSONObject(packetStr);
        String sql = json.getString("QV");

        sql = sql.trim();
        if (sql.endsWith(";") && sql.length() > 1 && sql.charAt(sql.length() - 2) == ';') {
          sql = sql.substring(0, sql.length() - 1);
        }

        return sql;
      } else {
        return packet.getRequestValue();
      }
    } catch (Exception e) {
      return packet.getRequestValue();
    }
  }

  private boolean executeSQL(String sql) {
    try {
      session.execute("USE \"" + keyspace + "\";");
      session.execute(sql);
      return true;
    } catch (InvalidQueryException e) {
      System.err.println("Invalid query: " + e.getMessage());

      try {
        createTableIfNotExists();
        session.execute("USE \"" + keyspace + "\";");
        session.execute(sql);
        return true;
      } catch (Exception retryException) {
        System.err.println("Recovery failed: " + retryException.getMessage());
        return false;
      }
    } catch (Exception e) {
      System.err.println("SQL execution failed: " + e.getMessage());
      return false;
    }
  }

  private void updateLatestSequence(String sql) {
    try {

      if (sql.contains("events+[")) {
        int start = sql.indexOf("events+[") + 8;
        int end = sql.indexOf("]", start);
        if (start < end) {
          String seqStr = sql.substring(start, end);
          long seq = Long.parseLong(seqStr);
          if (seq > latestExecutedSeq) {
            latestExecutedSeq = seq;
          }
        }
      }
    } catch (Exception e) {

    }
  }

  @Override
  public String checkpoint(String checkpointName) {
    System.out.println("Creating checkpoint: " + checkpointName);

    try {
      CheckpointState state = new CheckpointState();

      state.keyspace = this.keyspace;
      state.checkpointTime = System.currentTimeMillis();
      state.latestExecutedSeq = this.latestExecutedSeq;

      state.executedRequestIds = new ArrayList<>(this.executedRequests.keySet());

      state.databaseState = captureDatabaseState();

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(state);
      oos.close();

      String checkpointStr = Base64.getEncoder().encodeToString(baos.toByteArray());

      System.out.println("✓ Checkpoint created with:");
      System.out.println("  - " + state.executedRequestIds.size() + " executed requests");
      System.out.println("  - " + state.databaseState.size() + " database rows");
      System.out.println("  - Latest seq: " + state.latestExecutedSeq);

      return checkpointStr;

    } catch (Exception e) {
      System.err.println("✗ Error creating checkpoint: " + e.getMessage());
      e.printStackTrace();
      return "";
    }
  }

  private Map<Integer, List<Integer>> captureDatabaseState() {
    Map<Integer, List<Integer>> state = new HashMap<>();

    try {
      session.execute("USE \"" + keyspace + "\";");
      ResultSet resultSet = session.execute("SELECT id, events FROM grade");

      for (Row row : resultSet) {
        int id = row.getInt("id");
        List<Integer> events = new ArrayList<>(row.getList("events", Integer.class));
        state.put(id, events);
      }

      System.out.println("Captured " + state.size() + " rows from database");

    } catch (Exception e) {
      System.err.println("Failed to capture database state: " + e.getMessage());
    }

    return state;
  }

  private void restoreDatabaseState(Map<Integer, List<Integer>> dbState) {
    try {
      session.execute("USE \"" + keyspace + "\";");

      session.execute("TRUNCATE grade;");

      for (Map.Entry<Integer, List<Integer>> entry : dbState.entrySet()) {
        int id = entry.getKey();
        List<Integer> events = entry.getValue();

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO grade (id, events) VALUES (").append(id).append(", [");

        for (int i = 0; i < events.size(); i++) {
          if (i > 0) sb.append(", ");
          sb.append(events.get(i));
        }
        sb.append("]);");

        session.execute(sb.toString());
      }

      System.out.println("Restored " + dbState.size() + " rows to database");

    } catch (Exception e) {
      System.err.println("Failed to restore database state: " + e.getMessage());
      e.printStackTrace();
    }
  }

  @Override
  public boolean restore(String checkpointName, String checkpointState) {
    System.out.println("Restoring checkpoint: " + checkpointName);

    if (checkpointState == null || checkpointState.isEmpty()) {
      System.out.println("Empty checkpoint, starting fresh");
      return true;
    }

    try {

      byte[] decoded = Base64.getDecoder().decode(checkpointState);
      ByteArrayInputStream bais = new ByteArrayInputStream(decoded);
      ObjectInputStream ois = new ObjectInputStream(bais);
      CheckpointState state = (CheckpointState) ois.readObject();
      ois.close();

      executedRequests.clear();
      requestLog.clear();

      this.latestExecutedSeq = state.latestExecutedSeq;
      this.lastCheckpointTime = state.checkpointTime;

      for (String requestId : state.executedRequestIds) {
        executedRequests.put(requestId, true);
        requestLog.add(requestId);
      }

      if (state.databaseState != null && !state.databaseState.isEmpty()) {
        restoreDatabaseState(state.databaseState);
      }

      System.out.println("Checkpoint restored:");
      System.out.println("  - " + state.executedRequestIds.size() + " executed requests");
      System.out.println(
          "  - "
              + (state.databaseState != null ? state.databaseState.size() : 0)
              + " database rows");
      System.out.println("  - Latest seq: " + state.latestExecutedSeq);

      return true;

    } catch (IllegalArgumentException e) {

      if (checkpointState.equals("{}")) {
        System.out.println("Empty JSON checkpoint, starting fresh");
        return true;
      }
      System.err.println("Invalid checkpoint format (not Base64): " + e.getMessage());
      return false;
    } catch (Exception e) {
      System.err.println("Error restoring checkpoint: " + e.getMessage());
      e.printStackTrace();
      return false;
    }
  }

  public int getEventCount(int key) {
    try {
      session.execute("USE \"" + keyspace + "\";");
      ResultSet resultSet = session.execute("SELECT events FROM grade WHERE id = " + key + ";");

      Row row = resultSet.one();
      if (row != null) {
        return row.getList("events", Integer.class).size();
      }
      return 0;
    } catch (Exception e) {
      return 0;
    }
  }

  public Set<Integer> getAllKeys() {
    Set<Integer> keys = new HashSet<>();
    try {
      session.execute("USE \"" + keyspace + "\";");
      ResultSet resultSet = session.execute("SELECT id FROM grade;");

      for (Row row : resultSet) {
        keys.add(row.getInt("id"));
      }
    } catch (Exception e) {

    }
    return keys;
  }

  @Override
  public Request getRequest(String s) throws RequestParseException {
    try {
      return new RequestPacket(new JSONObject(s));
    } catch (Exception e) {
      try {
        return new RequestPacket(s.getBytes());
      } catch (Exception ex) {
        throw new RequestParseException(ex);
      }
    }
  }

  @Override
  public Set<IntegerPacketType> getRequestTypes() {
    return new HashSet<>(Collections.singletonList(RequestPacket.PaxosPacketType.PAXOS_PACKET));
  }

  public void close() {
    if (session != null && !session.isClosed()) {
      session.close();
    }
    if (cluster != null && !cluster.isClosed()) {
      cluster.close();
    }
    System.out.println("Closed for keyspace: " + keyspace);
  }

  private static class CheckpointState implements Serializable {
    private static final long serialVersionUID = 1L;

    String keyspace;
    long checkpointTime;
    long latestExecutedSeq;
    List<String> executedRequestIds;
    Map<Integer, List<Integer>> databaseState;

    public CheckpointState() {}
  }
}
