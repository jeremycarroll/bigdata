package com.bigdata.service.jini.master;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.bigdata.io.SerializerUtil;
import com.bigdata.service.ClientService;
import com.bigdata.service.IBigdataFederation;
import com.bigdata.service.IClientService;
import com.bigdata.service.IClientServiceCallable;
import com.bigdata.service.jini.JiniFederation;
import com.bigdata.service.jini.master.TaskMaster.JobState;
import com.bigdata.zookeeper.ZLock;
import com.bigdata.zookeeper.ZLockImpl;

//BTM - FOR_CLIENT_SERVICE
import com.bigdata.discovery.IBigdataDiscoveryManagement;
import com.bigdata.journal.IConcurrencyManager;
import com.bigdata.journal.IIndexManager;
import com.bigdata.journal.IScaleOutIndexStore;
import com.bigdata.resources.ILocalResourceManagement;
import com.bigdata.service.CallableExecutor;

/**
 * An abstract base class which may be used for client tasks run by the master
 * on one or more data services. This class contends for a {@link ZLock} based
 * on the assigned {@link #clientNum} and then invoked {@link #runWithZLock()}
 * if the {@link ZLock} if granted. If the lock is lost, it will continue to
 * contend for the lock and then run until finished.
 * <p>
 * Note: This implementation presumes that {@link #runWithZLock()} has some
 * means of understanding when it is done and can restart its work from where it
 * left off. One way to handle that is to write the state of the client into the
 * client's znode and to update that from time to time as the client makes
 * progress on its task. You can invoke {@link #setupClientState()} to do that.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 * @param <S>
 *            The generic for the {@link JobState}.
 * @param <U>
 *            The generic for the {@link Callable}'s return type.
 * @param <V>
 *            The generic type of the client state (stored in zookeeper).
 */
abstract public class AbstractClientTask<S extends TaskMaster.JobState, 
                                         U, V extends Serializable>
        implements IClientServiceCallable<U> {

    final protected static Logger log = Logger
            .getLogger(AbstractClientTask.class);

    protected final S jobState;

    protected final int clientNum;
    
    /**
     * Return the jobstate.
     */
    public S getJobState() {
        
        return jobState;
        
    }
    
    /**
     * Return the index assigned to the client.
     * 
     * @return The client index.
     */
    public int getClientNum() {
        
        return clientNum;
        
    }

    @Override
    public String toString() {

        return getClass().getName() + "{clientNum=" + clientNum + "}";

    }

    protected AbstractClientTask(final S jobState, final int clientNum) {

        this.jobState = jobState;

        this.clientNum = clientNum;

    }

    /**
     * Runs the generator.
     */
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE    public U startClientTask(IBigdataFederation federation,
//BTM - PRE_CLIENT_SERVICE                             ClientService clientService) throws Exception {
//BTM - PRE_CLIENT_SERVICE
    public U startClientTask(IIndexManager indexManager,
                             IConcurrencyManager concurrencyManager,
                             ILocalResourceManagement localResourceManager,
                             IBigdataDiscoveryManagement discoveryManager,
                             CallableExecutor embeddedCallableExecutor,
                             ZooKeeper zookeeperClient,
                             List<ACL> zookeeperAcl,
                             String zookeeperRoot)
             throws Exception
    {
//BTM - PRE_CLIENT_SERVICE -END

        if (log.isInfoEnabled())
            log.info("Running: client#=" + clientNum + ", " + jobState);

//BTM - PRE_CLIENT_SERVICE        JiniFederation jiniFederation = (JiniFederation) federation;
//BTM - PRE_CLIENT_SERVICE        final V clientState = setupClientState(jiniFederation);
        final V clientState = 
             setupClientState(zookeeperClient, zookeeperAcl, zookeeperRoot);
        
        while (true) {

            /**
             * The zpath for this client (set once the client starts executing
             * on the target {@link IClientService}). The data of this znode
             * is the client's state (if it saves its state in zookeeper).
             */
//BTM - PRE_CLIENT_SERVICE            String clientZPath = jobState.getClientZPath(jiniFederation, clientNum);
//BTM - PRE_CLIENT_SERVICE            List<ACL> acl = jiniFederation.getZooConfig().acl;
            String clientZPath =
                jobState.getClientZPath(zookeeperRoot, clientNum);

            /**
             * The zpath for the {@link ZLock} node. Only the instance of
             * this task holding the {@link ZLock} is allowed to run. This
             * makes it safe to run multiple instances of this task for the
             * same {@link #clientNum}. If one instance dies, the instance
             * that gains the {@link ZLock} will read the client's state
             * from zookeeper and continue processing.
             */
//BTM - PRE_CLIENT_SERVICE            ZLockImpl zlock = ZLockImpl.getLock(jiniFederation.getZookeeper(),
//BTM - PRE_CLIENT_SERVICE                    jobState.getLockNodeZPath(jiniFederation, clientNum), acl);
            ZLockImpl zlock =
                ZLockImpl.getLock
                    (zookeeperClient,
                     jobState.getLockNodeZPath(zookeeperRoot, clientNum),
                     zookeeperAcl);
            zlock.lock();
            try {
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE   final U ret = runWithZLock(clientState, jiniFederation, zlock, clientZPath);
                if (indexManager instanceof IScaleOutIndexStore) {
                    final U ret =
                        runWithZLock(clientState,
                                     (IScaleOutIndexStore)indexManager,
                                     localResourceManager,
                                     zlock, clientZPath);

                    if (log.isInfoEnabled()) {
                        log.info("Finished: client#=" + clientNum + ", "
                                 + jobState);
                    }
                    return ret;
                } else {
                    log.warn("AbstractClientTask.startClientTask: cannot "
                             +"execute runWithZlock [clientNum="+clientNum
                             +", jobState="+jobState+"] - indexManager NOT "
                             +"instance of IScaleOutIndexStore");
                    return null;
                }
//BTM - PRE_CLIENT_SERVICE - END

            } catch (SessionExpiredException ex) {

                /*
                 * Log warning and then try to re-obtain the zlock so we can
                 * finish the job.
                 */

                log.warn(this + " : will seek zlock again", ex);

                continue;

            } finally {

                zlock.unlock();

            }

        }

    }

    /**
     * Do work while holding the {@link ZLock}. The implementation SHOULD verify
     * from time to time that it in fact holds the {@link ZLock} using
     * {@link ZLock#isLockHeld()}.
     * 
     * @return The result.
     * 
     * @throws Exception
     * @throws KeeperException
     * @throws InterruptedException
     */
//BTM - PRE_CLIENT_SERVICE    abstract protected U runWithZLock(V clientState,
//BTM - PRE_CLIENT_SERVICE                                      JiniFederation jiniFederation,
//BTM - PRE_CLIENT_SERVICE                                      final ZLockImpl zlock,
//BTM - PRE_CLIENT_SERVICE                                      final String clientZPath)
//BTM - PRE_CLIENT_SERVICE            throws Exception, KeeperException, InterruptedException;
//BTM - PRE_CLIENT_SERVICE
    abstract protected U runWithZLock
                             (V clientState,
                              IScaleOutIndexStore indexStore,
                              ILocalResourceManagement localResourceManager,
                              final ZLockImpl zlock,
                              final String clientZPath)
             throws Exception, KeeperException, InterruptedException;

    /**
     * The method invoked {@link #newClientState()} and attempts to create the
     * client's znode with the serialized state as its data and that state will
     * be returned to the caller. If there is an existing znode for the client,
     * then the data of the znode is de-serialized and returned by this method.
     * <p>
     * This method is invoked automatically from within {@link #call()} before
     * the client attempts to obtain the {@link ZLock} (the zlock is a child of
     * the client's znode).
     * <p>
     * You can update the client's state from time to time using
     * {@link #writeClientState(Serializable)}. If the client looses the
     * {@link ZLock}, it can read the client state from zookeeper using this
     * method and pick up processing more or less where it left off (depending
     * on when you last updated the client state in zookeeper).
     * 
     * @return The client's state.
     * 
     * @throws InterruptedException
     * @throws KeeperException
     * 
     * @see JobState#getClientZPath(JiniFederation, int)
     */
    @SuppressWarnings("unchecked")
//BTM - PRE_CLIENT_SERVICE - BEGIN
//BTM - PRE_CLIENT_SERVICE    protected V setupClientState(JiniFederation jiniFederation)
//BTM - PRE_CLIENT_SERVICE            throws InterruptedException, KeeperException {
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        final ZooKeeper zookeeper =
//BTM - PRE_CLIENT_SERVICE                jiniFederation.getZookeeperAccessor().getZookeeper();
//BTM - PRE_CLIENT_SERVICE
//BTM - PRE_CLIENT_SERVICE        final String clientZPath = jobState.getClientZPath(jiniFederation,
//BTM - PRE_CLIENT_SERVICE                clientNum);
//BTM - PRE_CLIENT_SERVICE
    protected V setupClientState(final ZooKeeper zookeeper,
                             final List<ACL> zookeeperAcl,
                             final String zookeeperRoot)
                throws InterruptedException, KeeperException
    {
    final String clientZPath = jobState.getClientZPath(zookeeperRoot, clientNum);
//BTM - PRE_CLIENT_SERVICE - END

        V clientState;
        try {

            clientState = newClientState();

//BTM - PRE_CLIENT_SERVICE            zookeeper.create(clientZPath,
//BTM - PRE_CLIENT_SERVICE                             SerializerUtil.serialize(clientState),
//BTM - PRE_CLIENT_SERVICE                             jiniFederation.getZooConfig().acl,
//BTM - PRE_CLIENT_SERVICE                             CreateMode.PERSISTENT);
            zookeeper.create(clientZPath,
                             SerializerUtil.serialize(clientState),
                             zookeeperAcl,
                             CreateMode.PERSISTENT);

            if (log.isInfoEnabled())
                log.info("Created: clientZPath=" + clientZPath + ", state="
                        + clientState);

        } catch (NodeExistsException ex) {

            clientState = (V) SerializerUtil.deserialize(zookeeper.getData(
                    clientZPath, false, new Stat()));

            if (log.isInfoEnabled())
                log.info("Existing: clientZPath=" + clientZPath + ", state="
                        + clientState);

        }

        return clientState;
        
    }

    /**
     * Method updates the client state in zookeeper. The caller MUST be
     * holding the {@link ZLock} (this is verified).
     * 
     * @param clientState
     *            The state to be written into the znode identified by
     *            {@link #clientZPath}.
     * 
     * @throws InterruptedException
     * @throws KeeperException
     */
    protected void writeClientState(final V clientState, ZLockImpl zlock,
                                    String clientZPath)
            throws KeeperException, InterruptedException {

        if (clientState == null)
            throw new IllegalArgumentException();
        
        if (!zlock.isLockHeld())
            throw new InterruptedException("Lost ZLock");

        final ZooKeeper zookeeper = zlock.getZooKeeper();

        try {
            /*
             * Update the client state.
             */
            zookeeper.setData(clientZPath, SerializerUtil
                    .serialize(clientState), -1/* version */);
        } catch (ConnectionLossException ex) {
            /*
             * Note: There are a variety of transient errors which can
             * occur. Next time we are connected and and this method is
             * invoked we will update the client state, and that should be
             * good enough.
             */
            log.warn(ex);
        }

    }
    
    /**
     * Return a new instance of the client's state.
     */
    abstract protected V newClientState();

}
