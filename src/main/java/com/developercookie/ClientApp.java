package com.developercookie;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The type Client app which Connects to Zookeeper.
 */
public class ClientApp implements Watcher {
    private static final Logger logger = LoggerFactory.getLogger(ClientApp.class);
    // The Address in which Zookeeper is Started
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";

    //Zookeeper object which manages all the communication to the zookeeper
    private ZooKeeper zooKeeper;

    private static final int Session_Timeout = 3000;

    /**
     * The entry point of Client application.
     *
     * @param args the input arguments
     * @throws IOException          the io exception
     * @throws InterruptedException the interrupted exception
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        ClientApp clientApp = new ClientApp();
        clientApp.connectToZookeeper();
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
