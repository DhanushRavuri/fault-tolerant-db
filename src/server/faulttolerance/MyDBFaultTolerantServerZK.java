package server.faulttolerance;

import com.datastax.driver.core.*;
import edu.umass.cs.nio.*;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import server.ReplicatedServer;

public class MyDBFaultTolerantServerZK extends server.MyDBSingleServer {
  public static final int SLEEP = 100;
  public static final boolean DROP_TABLES_AFTER_TESTS = true;
  public static final int MAX_LOG_SIZE = 400;

  private ZooKeeper zk;
  private MessageNIOTransport<String, String> serverMessenger;
  private List<String> requestQueue = Collections.synchronizedList(new ArrayList<>());
  private Map<String, byte[]> requestData = new ConcurrentHashMap<>();
  private Map<String, NIOHeader> requestHeaders = new ConcurrentHashMap<>();
  private AtomicInteger lastExecutedIndex = new AtomicInteger(-1);
  private String myID;
  private NodeConfig<String> nodeConfig;
  private Thread requestProcessorThread;
  private volatile boolean running = true;

  private Cluster cluster;
  private Session cassandraSession;

  public MyDBFaultTolerantServerZK(
      NodeConfig<String> nodeConfig, String myID, InetSocketAddress isaDB) throws IOException {
    super(
        new InetSocketAddress(
            nodeConfig.getNodeAddress(myID),
            nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET),
        isaDB,
        myID);

    this.nodeConfig = nodeConfig;
    this.myID = myID;

    initCassandraSession(isaDB);

    this.serverMessenger =
        new MessageNIOTransport<String, String>(
            myID,
            nodeConfig,
            new AbstractBytePacketDemultiplexer() {
              @Override
              public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
                handleMessageFromServer(bytes, nioHeader);
                return true;
              }
            },
            true);

    log.log(
        Level.INFO,
        "Server {0} started on {1}",
        new Object[] {myID, this.clientMessenger.getListeningSocketAddress()});

    connectToZooKeeper();

    startRequestProcessor();

    recoverFromCrash();

    System.out.println("Fault-tolerant Server " + myID + " started and ready");
  }

  private void initCassandraSession(InetSocketAddress isaDB) {
    try {

      this.cluster =
          Cluster.builder().addContactPoint(isaDB.getHostName()).withPort(isaDB.getPort()).build();

      this.cassandraSession = cluster.connect(myID);

    } catch (Exception e) {
      log.log(Level.SEVERE, "Failed to connect to Cassandra", e);
      throw new RuntimeException("Cassandra connection failed", e);
    }
  }

  private void connectToZooKeeper() {
    CountDownLatch connectedLatch = new CountDownLatch(1);
    try {
      zk =
          new ZooKeeper(
              "localhost:2181",
              3000,
              event -> {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                  connectedLatch.countDown();
                }
              });
      connectedLatch.await();

      createZnodeIfNotExists("/requests", CreateMode.PERSISTENT);
      createZnodeIfNotExists("/servers", CreateMode.PERSISTENT);
      createZnodeIfNotExists("/checkpoints", CreateMode.PERSISTENT);

      zk.create(
          "/servers/server-",
          myID.getBytes(),
          ZooDefs.Ids.OPEN_ACL_UNSAFE,
          CreateMode.EPHEMERAL_SEQUENTIAL);

      watchForNewRequests();

    } catch (Exception e) {
      throw new RuntimeException("Failed to connect to ZooKeeper", e);
    }
  }

  private void createZnodeIfNotExists(String path, CreateMode mode)
      throws KeeperException, InterruptedException {
    if (zk.exists(path, false) == null) {
      try {
        zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
      } catch (KeeperException.NodeExistsException e) {

      }
    }
  }

  private void watchForNewRequests() throws KeeperException, InterruptedException {
    zk.getChildren(
        "/requests",
        new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
              try {
                updateRequestQueue();
                watchForNewRequests();
              } catch (Exception e) {
                e.printStackTrace();
              }
            }
          }
        });
  }

  private void updateRequestQueue() throws KeeperException, InterruptedException {
    List<String> children = zk.getChildren("/requests", false);
    Collections.sort(children);

    synchronized (requestQueue) {
      for (String child : children) {
        String fullPath = "/requests/" + child;
        if (!requestQueue.contains(fullPath)) {
          requestQueue.add(fullPath);
          byte[] data = zk.getData(fullPath, false, null);
          requestData.put(fullPath, data);
        }
      }

      Collections.sort(requestQueue);

      if (requestQueue.size() > MAX_LOG_SIZE) {
        cleanupOldRequests();
      }
    }
  }

  private void cleanupOldRequests() {
    int currentIndex = lastExecutedIndex.get();
    int safeToDeleteUpTo = Math.max(0, currentIndex - (MAX_LOG_SIZE / 2));

    Iterator<String> it = requestQueue.iterator();
    int i = 0;
    while (it.hasNext() && i < safeToDeleteUpTo) {
      String path = it.next();
      try {
        zk.delete(path, -1);
        it.remove();
        requestData.remove(path);
        requestHeaders.remove(path);
      } catch (Exception e) {

      }
      i++;
    }
  }

  private void startRequestProcessor() {
    requestProcessorThread =
        new Thread(
            () -> {
              while (running) {
                try {
                  processNextRequest();
                  Thread.sleep(SLEEP);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  break;
                } catch (Exception e) {
                  e.printStackTrace();
                }
              }
            });
    requestProcessorThread.setName("RequestProcessor-" + myID);
    requestProcessorThread.start();
  }

  private void processNextRequest() {
    synchronized (requestQueue) {
      try {
        int currentIndex = lastExecutedIndex.get();

        if (!requestQueue.isEmpty()) {

          String firstRequestPath = requestQueue.get(0);
          byte[] requestBytes = requestData.get(firstRequestPath);

          if (requestBytes != null) {

            NIOHeader header = requestHeaders.get(firstRequestPath);

            executeOnDB(requestBytes, header);

            lastExecutedIndex.incrementAndGet();

            requestQueue.remove(0);
            requestData.remove(firstRequestPath);
            requestHeaders.remove(firstRequestPath);

            try {
              zk.delete(firstRequestPath, -1);
            } catch (KeeperException.NoNodeException e) {

            } catch (Exception e) {

            }

            checkpoint();
          }
        }
      } catch (Exception e) {

        System.err.println("Error in processNextRequest: " + e.getMessage());
      }
    }
  }

  private void executeOnDB(byte[] request, NIOHeader header) {
    String requestStr = new String(request).trim();

    try {

      cassandraSession.execute(requestStr);

      if (header != null && header.sndr != null) {
        String response = "[SUCCESS] Executed: " + requestStr;
        serverMessenger.send(header.sndr, response.getBytes());
      }

    } catch (Exception e) {

      if (header != null && header.sndr != null) {
        try {
          String error = "[ERROR] Failed to execute: " + e.getMessage();
          serverMessenger.send(header.sndr, error.getBytes());
        } catch (IOException ioException) {
          ioException.printStackTrace();
        }
      }
    }
  }

  private void checkpoint() {
    try {

      String checkpointPath = "/checkpoints/" + myID;
      byte[] checkpointData = String.valueOf(lastExecutedIndex.get()).getBytes();

      if (zk.exists(checkpointPath, false) != null) {
        zk.setData(checkpointPath, checkpointData, -1);
      } else {
        zk.create(
            checkpointPath, checkpointData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }

    } catch (Exception e) {

    }
  }

  private void recoverFromCrash() {
    try {

      String checkpointPath = "/checkpoints/" + myID;
      Stat stat = zk.exists(checkpointPath, false);

      if (stat != null) {
        byte[] data = zk.getData(checkpointPath, false, null);
        int savedIndex = Integer.parseInt(new String(data));
        lastExecutedIndex.set(savedIndex);
        System.out.println("Server " + myID + " recovered checkpoint: " + savedIndex);
      } else {
        System.out.println("Server " + myID + " no checkpoint found");
        lastExecutedIndex.set(-1);
      }

      updateRequestQueue();

      System.out.println("Server " + myID + " queue size after recovery: " + requestQueue.size());

    } catch (Exception e) {
      System.err.println("Server " + myID + " recovery error: " + e.getMessage());
      lastExecutedIndex.set(-1);
    }
  }

  @Override
  protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
    try {

      String requestPath =
          zk.create(
              "/requests/request-",
              bytes,
              ZooDefs.Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT_SEQUENTIAL);

      requestHeaders.put(requestPath, header);

      System.out.println("Server " + myID + " received request: " + requestPath);

      if (header != null && header.sndr != null) {
        String ack = "[ACK] Request queued at: " + requestPath;
        serverMessenger.send(header.sndr, ack.getBytes());
      }

      updateRequestQueue();

    } catch (Exception e) {
      System.err.println("Server " + myID + " error handling client request: " + e.getMessage());

      if (header != null && header.sndr != null) {
        try {
          String error = "[ERROR] Failed to process request: " + e.getMessage();
          serverMessenger.send(header.sndr, error.getBytes());
        } catch (IOException ioException) {
          ioException.printStackTrace();
        }
      }
    }
  }

  protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {}

  @Override
  public void close() {
    running = false;

    if (requestProcessorThread != null) {
      requestProcessorThread.interrupt();
      try {
        requestProcessorThread.join(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    try {
      if (zk != null) zk.close();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    if (cassandraSession != null) {
      cassandraSession.close();
    }

    if (cluster != null) {
      cluster.close();
    }

    if (serverMessenger != null) {
      serverMessenger.stop();
    }

    super.close();

    System.out.println("Server " + myID + " closed");
  }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println(
                    "Usage: java MyDBFaultTolerantServerZK <server.properties> <myID>"
                            + " [<cassandra_host:port>]");
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
