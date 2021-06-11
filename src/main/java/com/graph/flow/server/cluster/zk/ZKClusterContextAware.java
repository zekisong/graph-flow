package com.graph.flow.server.cluster.zk;

import com.graph.flow.config.FlowConfig;
import com.graph.flow.constant.FlowConstant;
import com.graph.flow.exception.FlowRuntimeException;
import com.graph.flow.proto.ClusterContext;
import com.graph.flow.proto.Node;
import com.graph.flow.server.cluster.ClusterContextAware;
import com.graph.flow.utils.NetUtils;
import com.graph.flow.utils.NodeUtils;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKClusterContextAware implements ClusterContextAware {

    private static final Logger LOG = LoggerFactory.getLogger(ZKClusterContextAware.class);
    private ZooKeeper zkClient;
    private Node myself;
    private ClusterContext current;
    private String rootDir;

    public ZKClusterContextAware() {
        init();
    }

    @Override
    public ClusterContext snapshot() {
        while (true) {
            if (current == null) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOG.warn("context not prepare, sleep 1s and retry...");
                }
            } else {
                return current.toBuilder().build();
            }
        }
    }

    @Override
    public Node getMySelf() {
        return myself;
    }

    @Override
    public void start() {
        String clusterName = FlowConfig.getInstance().get(FlowConstant.CLUSTER_NAME,
                FlowConstant.DEFAULT_CLUSTER_NAME);
        rootDir = "/" + clusterName;
        String nodeDir = rootDir + "/nodes";
        String taskDir = rootDir + "/tasks";
        while (true) {
            try {
                if (zkClient.exists(rootDir, null) == null) {
                    zkClient.create(rootDir, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                if (zkClient.exists(nodeDir, null) == null) {
                    zkClient.create(nodeDir, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                if (zkClient.exists(taskDir, null) == null) {
                    zkClient.create(taskDir, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                zkClient.getChildren(nodeDir, new NodeWatcher(this, nodeDir), null);
                registerMySelf();
                break;

            } catch (Exception e) {
                LOG.warn("init zk env failed! retry after 1000ms...", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    throw new RuntimeException("interrupt!", ie);
                }
            }
        }
    }

    @Override
    public void stop() {
        try {
            zkClient.close();
        } catch (InterruptedException e) {
            LOG.info("zk client close failed! ignore...");
        }
    }

    private void init() {
        FlowConfig conf = FlowConfig.getInstance();
        try {
            newZkClient();
            Socket socket = new Socket();
            String zkAddr = conf.get(FlowConstant.CLUSTER_ZK_ADDR, FlowConstant.DEFAULT_CLUSTER_ZK_ADDR);
            String[] items = zkAddr.split(":");
            socket.connect(new InetSocketAddress(items[0], Integer.valueOf(items[1])));
            String localAddress = socket.getLocalAddress().getHostAddress();
            int localPort = NetUtils.getFreeSocketPort();
            this.myself = Node.newBuilder().setAddress(localAddress).setPort(localPort).build();
        } catch (IOException e) {
            throw new FlowRuntimeException("zk client init failed!", e);
        }
    }

    public void registerMySelf() throws KeeperException, InterruptedException {
        zkClient.create(String.format("%s/nodes/%s:%d", rootDir, myself.getAddress(), myself.getPort()),
                null,
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        LOG.info(String.format("register node:%s success!", NodeUtils.toString(myself)));
    }

    public ZooKeeper getZkClient() {
        return zkClient;
    }

    public void newZkClient() throws IOException {
        FlowConfig conf = FlowConfig.getInstance();
        String zkAddr = conf.get(FlowConstant.CLUSTER_ZK_ADDR, FlowConstant.DEFAULT_CLUSTER_ZK_ADDR);
        int zkTimeout = conf.get(FlowConstant.CLUSTER_ZK_TIMEOUT, FlowConstant.DEFAULT_CLUSTER_ZK_TIMEOUT);
        zkClient = new ZooKeeper(zkAddr, zkTimeout, event -> {
        });
    }

    public void setCurrent(ClusterContext current) {
        this.current = current;
    }
}
