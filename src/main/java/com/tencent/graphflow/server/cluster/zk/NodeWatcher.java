package com.tencent.graphflow.server.cluster.zk;

import com.tencent.graphflow.utils.NodeUtils;
import com.tencent.tdf.proto.ClusterContext;
import com.tencent.tdf.proto.Node;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeWatcher implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(NodeWatcher.class);
    private ZKClusterContextAware zkAware;
    private String workDir;

    public NodeWatcher(ZKClusterContextAware zkAware, String workDir) {
        this.zkAware = zkAware;
        this.workDir = workDir;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        while (true) {
            try {
                List<String> children = zkAware.getZkClient().getChildren(workDir, this);
                List<Node> nodes = children.stream()
                        .map(c -> NodeUtils.fromString(c))
                        .filter(n -> n != null)
                        .collect(Collectors.toList());
                nodes.sort(Comparator.comparingInt(Node::hashCode));
                if (nodes.indexOf(zkAware.getMySelf()) < 0) {
                    LOG.warn(String.format("node:%s lost connected with zk, retry connect...",
                            NodeUtils.toString(zkAware.getMySelf())));
                    zkAware.registerMySelf();
                }
                ClusterContext current = ClusterContext.newBuilder()
                        .addAllNodes(nodes)
                        .setTimestamp(System.currentTimeMillis())
                        .build();
                zkAware.setCurrent(current);
                break;
            } catch (SessionExpiredException | ConnectionLossException see) {
                try {
                    zkAware.newZkClient();
                } catch (IOException e) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        ie.printStackTrace();
                    }
                }
            } catch (Exception e) {
                LOG.warn(String.format("get children of %s failed! retry after 1s.", workDir), e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException interruptedException) {
                    interruptedException.printStackTrace();
                }
            }
        }
    }
}
