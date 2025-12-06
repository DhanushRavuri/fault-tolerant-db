package server.faulttolerance;

import com.datastax.driver.core.*;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import server.ReplicatedServer;

public class MyDBFaultTolerantServerZK extends server.MyDBSingleServer implements Watcher {

  public static final int SLEEP = 1000;
  public static final boolean DROP_TABLES_AFTER_TESTS = true;
  public static final int MAX_LOG_SIZE = 400;
  public static final String ZK_HOST = "localhost:2181";

  private ZooKeeper zk;
  private Cluster cluster;
  private Session session;
  private final String myID;

  private static final String BASE_PATH = "/ftdb";
  private static final String REQUESTS_PATH = BASE_PATH + "/requests";
  private static final String CHECKPOINT_PREFIX = BASE_PATH + "/checkpoint";
  private static final String LAST_SEQ_PATH = BASE_PATH + "/last_seq";

  private long lastProcessedSeq = -1;
  private final AtomicLong operationCounter = new AtomicLong(0);
  private final Set<String> processedRequests = ConcurrentHashMap.newKeySet();
  private boolean recovering = false;

  public MyDBFaultTolerantServerZK(
      NodeConfig<String> nodeConfig, String myID, InetSocketAddress isaDB) throws IOException {
    super(
        new InetSocketAddress(
            nodeConfig.getNodeAddress(myID),
            nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET),
        isaDB,
        myID);

    this.myID = myID;

    try {

      initZooKeeper();

      initCassandra();

      recover();

      new Thread(this::processRequests, "Processor-" + myID).start();

      System.out.println("[" + myID + "] Server ready");

    } catch (Exception e) {
      System.err.println("[" + myID + "] Initialization failed: " + e.getMessage());
      e.printStackTrace();
      throw new IOException(e);
    }
  }

  private void initZooKeeper() throws IOException, KeeperException, InterruptedException {
    this.zk = new ZooKeeper(ZK_HOST, 10000, this);

    int attempts = 0;
    while (zk.getState() != ZooKeeper.States.CONNECTED && attempts < 30) {
      Thread.sleep(100);
      attempts++;
    }

    if (zk.getState() != ZooKeeper.States.CONNECTED) {
      throw new IOException("Failed to connect to ZooKeeper");
    }

    createIfNotExists(BASE_PATH);
    createIfNotExists(REQUESTS_PATH);
    createIfNotExists(CHECKPOINT_PREFIX);
    createIfNotExists(LAST_SEQ_PATH);
  }

  private void createIfNotExists(String path) throws KeeperException, InterruptedException {
    if (zk.exists(path, false) == null) {
      try {
        zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } catch (KeeperException.NodeExistsException ignored) {

      }
    }
  }

  private void initCassandra() {
    this.cluster = Cluster.builder().addContactPoint("localhost").build();

    this.session = cluster.connect(myID);

    session.execute(
        "CREATE TABLE IF NOT EXISTS grade (" + "id int PRIMARY KEY, " + "events list<int>" + ")");
  }

  private void recover() {
    try {
      System.out.println("[" + myID + "] Starting recovery...");
      recovering = true;

      Stat stat = new Stat();
      byte[] lastSeqData = zk.getData(LAST_SEQ_PATH, false, stat);
      long checkpointSeq = -1;

      if (lastSeqData != null && lastSeqData.length > 0) {
        checkpointSeq = Long.parseLong(new String(lastSeqData, StandardCharsets.UTF_8));
        System.out.println("[" + myID + "] Found checkpoint at seq: " + checkpointSeq);

        String checkpointPath = CHECKPOINT_PREFIX + "/" + checkpointSeq;
        if (zk.exists(checkpointPath, false) != null) {
          byte[] checkpointData = zk.getData(checkpointPath, false, null);
          if (checkpointData != null && checkpointData.length > 0) {
            restoreCheckpoint(checkpointData);
          }
        }

        lastProcessedSeq = checkpointSeq;
      }

      System.out.println("[" + myID + "] Recovery complete. Last seq: " + lastProcessedSeq);
      recovering = false;

    } catch (Exception e) {
      System.err.println("[" + myID + "] Recovery error: " + e.getMessage());
      recovering = false;
    }
  }

  private void restoreCheckpoint(byte[] data) {
    try {
      ByteArrayInputStream bis = new ByteArrayInputStream(data);
      ObjectInputStream ois = new ObjectInputStream(bis);

      session.execute("TRUNCATE grade");

      int numRecords = ois.readInt();
      System.out.println("[" + myID + "] Restoring " + numRecords + " records");

      for (int i = 0; i < numRecords; i++) {
        int id = ois.readInt();
        int listSize = ois.readInt();
        List<Integer> events = new ArrayList<>();

        for (int j = 0; j < listSize; j++) {
          events.add(ois.readInt());
        }

        PreparedStatement ps = session.prepare("INSERT INTO grade (id, events) VALUES (?, ?)");
        session.execute(ps.bind(id, events));
      }

      ois.close();

    } catch (Exception e) {
      System.err.println("[" + myID + "] Checkpoint restore failed: " + e.getMessage());
    }
  }

  @Override
  protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
    String request = new String(bytes, StandardCharsets.UTF_8).trim();
    System.out.println("[" + myID + "] Received: " + request);

    try {

      String requestPath =
          zk.create(
              REQUESTS_PATH + "/req-",
              bytes,
              ZooDefs.Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT_SEQUENTIAL);

      System.out.println("[" + myID + "] Created: " + requestPath);

      this.clientMessenger.send(header.sndr, "ACK".getBytes(StandardCharsets.UTF_8));

    } catch (Exception e) {
      System.err.println("[" + myID + "] Request handling failed: " + e.getMessage());
      e.printStackTrace();
    }
  }

  private void processRequests() {
    while (true) {
      try {
        if (recovering) {
          Thread.sleep(100);
          continue;
        }

        List<String> requests = zk.getChildren(REQUESTS_PATH, true);
        if (requests.isEmpty()) {
          Thread.sleep(100);
          continue;
        }

        requests.sort(String::compareTo);

        synchronized (this) {
          for (String reqNode : requests) {

            long seqNum = extractSeqNumber(reqNode);

            if (seqNum <= lastProcessedSeq || processedRequests.contains(reqNode)) {
              continue;
            }

            if (seqNum == lastProcessedSeq + 1) {
              processRequest(reqNode);
              lastProcessedSeq = seqNum;
              processedRequests.add(reqNode);

              if (operationCounter.incrementAndGet() % 50 == 0) {
                createCheckpoint();
              }
            } else if (seqNum > lastProcessedSeq + 1) {

              break;
            }
          }
        }

        cleanupOldRequests();

        Thread.sleep(10);

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        System.err.println("[" + myID + "] Processing error: " + e.getMessage());
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          break;
        }
      }
    }
  }

  private long extractSeqNumber(String reqNode) {
    try {

      return Long.parseLong(reqNode.substring(4));
    } catch (Exception e) {
      return -1;
    }
  }

  private void processRequest(String reqNode) {
    try {
      String path = REQUESTS_PATH + "/" + reqNode;
      byte[] data = zk.getData(path, false, null);

      if (data == null || data.length == 0) {
        return;
      }

      String query = new String(data, StandardCharsets.UTF_8).trim();
      System.out.println("[" + myID + "] Executing: " + query);

      executeQuery(query);

    } catch (Exception e) {
      System.err.println("[" + myID + "] Request execution failed: " + e.getMessage());
    }
  }

  private void executeQuery(String query) {
    try {
      if (query.toLowerCase().startsWith("insert")) {

        int start = query.indexOf("values (") + 8;
        int end = query.indexOf(",", start);
        String idStr = query.substring(start, end).trim();
        int id = Integer.parseInt(idStr);

        PreparedStatement ps = session.prepare("INSERT INTO grade (id, events) VALUES (?, ?)");
        session.execute(ps.bind(id, new ArrayList<Integer>()));

      } else if (query.toLowerCase().startsWith("update")) {

        int plusIndex = query.indexOf("events+[");
        int bracketEnd = query.indexOf("]", plusIndex);
        String eventStr = query.substring(plusIndex + 8, bracketEnd).trim();
        int eventValue = Integer.parseInt(eventStr);

        int whereIndex = query.indexOf("where id=");
        String idStr = query.substring(whereIndex + 9).replace(";", "").trim();
        int id = Integer.parseInt(idStr);

        PreparedStatement selectPs = session.prepare("SELECT events FROM grade WHERE id = ?");
        ResultSet rs = session.execute(selectPs.bind(id));
        Row row = rs.one();

        List<Integer> events;
        if (row != null) {
          events = new ArrayList<>(row.getList("events", Integer.class));
        } else {
          events = new ArrayList<>();
        }

        events.add(eventValue);

        PreparedStatement updatePs = session.prepare("UPDATE grade SET events = ? WHERE id = ?");
        session.execute(updatePs.bind(events, id));
      }

    } catch (Exception e) {
      System.err.println("[" + myID + "] Query failed: " + query + " - " + e.getMessage());

      try {
        session.execute(query);
      } catch (Exception e2) {
        System.err.println("[" + myID + "] Raw query also failed: " + e2.getMessage());
      }
    }
  }

  private void createCheckpoint() {
    try {
      System.out.println("[" + myID + "] Creating checkpoint at seq " + lastProcessedSeq);

      ResultSet rs = session.execute("SELECT * FROM grade");
      List<Row> rows = rs.all();

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(bos);

      oos.writeInt(rows.size());

      for (Row row : rows) {
        int id = row.getInt("id");
        List<Integer> events = row.getList("events", Integer.class);

        oos.writeInt(id);
        oos.writeInt(events.size());
        for (Integer event : events) {
          oos.writeInt(event);
        }
      }

      oos.flush();
      byte[] checkpointData = bos.toByteArray();
      oos.close();

      String checkpointPath = CHECKPOINT_PREFIX + "/" + lastProcessedSeq;
      if (zk.exists(checkpointPath, false) == null) {
        zk.create(
            checkpointPath, checkpointData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } else {
        zk.setData(checkpointPath, checkpointData, -1);
      }

      zk.setData(
          LAST_SEQ_PATH, Long.toString(lastProcessedSeq).getBytes(StandardCharsets.UTF_8), -1);

      System.out.println("[" + myID + "] Checkpoint saved with " + rows.size() + " records");

    } catch (Exception e) {
      System.err.println("[" + myID + "] Checkpoint failed: " + e.getMessage());
    }
  }

  private void cleanupOldRequests() {
    try {
      List<String> requests = zk.getChildren(REQUESTS_PATH, false);
      if (requests.size() > MAX_LOG_SIZE) {
        requests.sort(String::compareTo);
        int toDelete = requests.size() - MAX_LOG_SIZE;

        for (int i = 0; i < toDelete; i++) {
          try {
            zk.delete(REQUESTS_PATH + "/" + requests.get(i), -1);
            processedRequests.remove(requests.get(i));
          } catch (KeeperException.NoNodeException ignored) {

          }
        }
      }
    } catch (Exception ignored) {

    }
  }

  @Override
  public void process(WatchedEvent event) {}

  @Override
  public void close() {
    System.out.println("[" + myID + "] Shutting down...");

    if (operationCounter.get() > 0) {
      createCheckpoint();
    }

    try {
      if (zk != null) {
        zk.close();
      }
      if (session != null) {
        session.close();
      }
      if (cluster != null) {
        cluster.close();
      }
    } catch (Exception e) {
      System.err.println("[" + myID + "] Shutdown error: " + e.getMessage());
    }

    super.close();
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.err.println("Usage: MyDBFaultTolerantServerZK <config> <myID> [dbAddress]");
      System.exit(1);
    }

    new MyDBFaultTolerantServerZK(
        NodeConfigUtils.getNodeConfigFromFile(
            args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer.SERVER_PORT_OFFSET),
        args[1],
        args.length > 2
            ? Util.getInetSocketAddressFromString(args[2])
            : new InetSocketAddress("localhost", 9042));
  }
}
