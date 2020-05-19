package com.developercookie;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * The type Client app which Connects to Zookeeper.
 */
public class ClientApp implements Watcher {
    private static final Logger logger = LoggerFactory.getLogger(ClientApp.class);
    // The Address in which Zookeeper is Started
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final String DEFAULT_ELECTION_ZNODE_Path = "/election";
    private String currentClientAppZnodeName;
    //Zookeeper object which manages all the communication to the zookeeper
    private ZooKeeper zooKeeper;

    private static final int Session_Timeout = 3000;

    /**
     * The entry point of Client application.
     *
     * @param args the input arguments
     * @throws IOException          the io exception
     * @throws InterruptedException the interrupted exception
     * @throws KeeperException      the keeper exception
     */
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ClientApp clientApp = new ClientApp();
        clientApp.connectToZookeeper();
        clientApp.createZnodeNameSpace();
        clientApp.volunterForLeaderRole();
        clientApp.electLeader();
        clientApp.run();
        clientApp.close();
        logger.warn("Successfully Disconnect from Zookeeper");
    }

    /**
     * Connect to zookeeper.Creating the Zookeeper Object to Communicate with Zookeeper
     *
     * @throws IOException the io exception
     */
    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, Session_Timeout, this);

    }

    /**
     * Create znode name space.
     *
     * @throws KeeperException      the keeper exception
     * @throws InterruptedException the interrupted exception
     */
    public void createZnodeNameSpace() throws KeeperException, InterruptedException {
        if (zooKeeper.exists(DEFAULT_ELECTION_ZNODE_Path, false) == null)
            this.zooKeeper.create(DEFAULT_ELECTION_ZNODE_Path, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * Volunter for leader role.
     *
     * @throws KeeperException      the keeper exception
     * @throws InterruptedException the interrupted exception
     */
    public void volunterForLeaderRole() throws KeeperException, InterruptedException {
        String znodePrefix = DEFAULT_ELECTION_ZNODE_Path + "/c_";
        String zNodeFullPathCreated = this.zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        this.currentClientAppZnodeName = zNodeFullPathCreated.replace(DEFAULT_ELECTION_ZNODE_Path + "/", "");
    }

    /**
     * Elect leader. if the Election Node is Smallest then make it as Leader
     *
     * @throws KeeperException      the keeper exception
     * @throws InterruptedException the interrupted exception
     */
    public void electLeader() throws KeeperException, InterruptedException {
        List<String> allClientapps = zooKeeper.getChildren(DEFAULT_ELECTION_ZNODE_Path, false);
        Collections.sort(allClientapps);
        String firstClientApp = allClientapps.get(0);
        if (this.currentClientAppZnodeName.equals(firstClientApp)) {
            logger.warn("I am the Leader");
        } else {
            logger.warn("I am not the Leader Elected Leader is {} ", firstClientApp);
        }
    }

    /**
     * Closing the Zookeeper Connection.
     *
     * @throws InterruptedException the interrupted exception
     */
// Closing
    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    /**
     * Wait till the Zookeeper Event Thread is kept open .
     *
     * @throws InterruptedException the interrupted exception
     */
    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    // The Process Method will be Called by Zookeeper Library in a Separate Event Thread.
    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    logger.warn("Successfully Connected to Zookeeper");
                } else {
                    synchronized (zooKeeper) {
                        zooKeeper.notifyAll();
                    }
                }
        }
    }


}
