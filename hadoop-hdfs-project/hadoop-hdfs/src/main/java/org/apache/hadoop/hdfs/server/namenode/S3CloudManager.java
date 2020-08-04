package org.apache.hadoop.hdfs.server.namenode;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.DeleteVersionRequest;
import com.google.common.base.Preconditions;
import io.hops.common.INodeUtil;
import io.hops.leader_election.node.ActiveNode;
import io.hops.leader_election.node.SortedActiveNodeList;
import io.hops.metadata.HdfsStorageFactory;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.metadata.hdfs.dal.S3ObjectDeletableDataAccess;
import io.hops.metadata.hdfs.dal.S3ObjectInfoDataAccess;
import io.hops.metadata.hdfs.dal.S3ProcessableDataAccess;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.ProjectedINode;
import io.hops.metadata.hdfs.entity.S3ObjectDeletable;
import io.hops.metadata.hdfs.entity.S3Processable;
import io.hops.transaction.handler.HDFSOperationType;
import io.hops.transaction.handler.HopsTransactionalRequestHandler;
import io.hops.transaction.handler.LightWeightRequestHandler;
import io.hops.transaction.lock.LockFactory;
import io.hops.transaction.lock.TransactionLockTypes;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.S3DownloadInputStream;
import org.apache.hadoop.hdfs.S3UploadOutputStream;
import org.apache.hadoop.hdfs.protocol.RangedS3Object;
import org.apache.hadoop.hdfs.protocol.S3File;
import org.apache.hadoop.hdfs.server.cloud.S3ObjectInfoContiguous;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.util.concurrent.BlockingThreadPoolExecutorService;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hdfs.DFSConfigKeys.*;

public class S3CloudManager extends Thread {
  private static final Logger LOG = Logger.getLogger(S3CloudManager.class);
  private final NameNode nn;
  private final Configuration conf;
  private boolean isLeader;
  private boolean running;
  private DeleteLeader deleteLeader;
  private DeleteWorker deleteWorker;
  private ProcessableLeader processableLeader;
  private ProcessableWorker processableWorker;
  private final ExecutorService threadPoolExecutor;
  private final AmazonS3 s3;
  private final boolean crc32Enabled;
  private static final long TERABYTE = 1024L * 1024L * 1024L * 1024L;

  public S3CloudManager(NameNode nn, Configuration conf) {
    this.crc32Enabled = conf.getBoolean(DFS_NAMENODE_OBJECT_STORAGE_S3_CRC32_ENABLED_KEY,
            DFS_NAMENODE_OBJECT_STORAGE_S3_CRC32_ENABLED_DEFAULT);
    this.nn = nn;
    this.conf = conf;
    this.isLeader = false;
    this.running = !crc32Enabled;
    final String region = conf.get(DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_S3_BUCKET_REGION_KEY, null);
    final String bucket = conf.get(DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_S3_BUCKET_KEY, null);
    Preconditions.checkNotNull(region, "S3 bucket region not configured.");
    Preconditions.checkNotNull(region, "S3 bucket not configured.");
    this.s3 = AmazonS3ClientBuilder.standard()
            .withCredentials(new EnvironmentVariableCredentialsProvider())
            .withRegion(Regions.fromName(region))
            .build();
    String bucketVersionStatus = s3.getBucketVersioningConfiguration(bucket).getStatus();
    Preconditions.checkArgument(bucketVersionStatus.equals(BucketVersioningConfiguration.ENABLED),
            "Configured S3 bucket is not versioned.");
    int maxThreads = conf.getInt(DFS_CLIENT_OBJECT_STORAGE_MAX_THREADS_KEY, DFS_CLIENT_OBJECT_STORAGE_MAX_THREADS_DEFAULT);
    int totalTasks = conf.getInt(DFS_CLIENT_OBJECT_STORAGE_MAX_TASKS_KEY, DFS_CLIENT_OBJECT_STORAGE_MAX_TASKS_DEFAULT);
    int keepAliveTime = conf.getInt(DFS_CLIENT_OBJECT_STORAGE_KEEP_ALIVE_KEY, DFS_CLIENT_OBJECT_STORAGE_KEEP_ALIVE_DEFAULT);
    threadPoolExecutor = new BlockingThreadPoolExecutorService(maxThreads, totalTasks, keepAliveTime, TimeUnit.SECONDS,
            "hopsfs-s3-manager");
  }

  @Override
  public void run() {
    deleteWorker = new DeleteWorker();
    deleteWorker.start();
    processableWorker = new ProcessableWorker();
    processableWorker.start();

    while(running) {
      boolean isLeaderNow = nn.isLeader();

      // Become new leader
      if(isLeaderNow && !isLeader) {
        deleteLeader = new DeleteLeader();
        deleteLeader.start();
        processableLeader = new ProcessableLeader();
        processableLeader.start();
        isLeader = true;
      } else if(!isLeaderNow && isLeader) { // No longer leader
        isLeader = false;
        try {
          stopLeaders();
        } catch (InterruptedException e) {
          running = false;
          continue;
        }
      }

      try {
        Thread.sleep(getLeaderCheckPeriod());
      } catch (InterruptedException e) {
        running = false;
      }
    }

    LOG.info("Main loop terminated, proceeding to stop worker and leader threads.");
    try {
      stopInternal();
    } catch (InterruptedException e) {}
  }

  public synchronized boolean isRunning() {
    return running;
  }

  private static S3ObjectDeletableDataAccess<S3ObjectDeletable> getDeletableDataAccess() {
    S3ObjectDeletableDataAccess<S3ObjectDeletable> da =
            (S3ObjectDeletableDataAccess<S3ObjectDeletable>)HdfsStorageFactory.getDataAccess(S3ObjectDeletableDataAccess.class);
    if(da == null) {
      throw new RuntimeException("null S3ObjectDeletableDataAccess obtained.");
    }
    return da;
  }

  private static S3ProcessableDataAccess<S3Processable> getProcessableDataAccess() {
    S3ProcessableDataAccess<S3Processable> da =
            (S3ProcessableDataAccess<S3Processable>)HdfsStorageFactory.getDataAccess(S3ProcessableDataAccess.class);
    if(da == null) {
      throw new RuntimeException("null S3ProcessableDataAccess obtained.");
    }
    return da;
  }

  private static S3ObjectInfoDataAccess<S3ObjectInfoContiguous> getObjectDataAccess() {
    S3ObjectInfoDataAccess<S3ObjectInfoContiguous> da =
            (S3ObjectInfoDataAccess<S3ObjectInfoContiguous>)HdfsStorageFactory.getDataAccess(S3ObjectInfoDataAccess.class);
    if(da == null) {
      throw new RuntimeException("null S3ObjectInfoDataAccess obtained.");
    }
    return da;
  }

  private static INodeDataAccess<INode> getInodeDataAccess() {
    INodeDataAccess<INode> da = (INodeDataAccess<INode>)HdfsStorageFactory.getDataAccess(INodeDataAccess.class);
    if(da == null) {
      throw new RuntimeException("null INodeDataAccess obtained.");
    }
    return da;
  }

  // Creates an S3Processable record if one does not exist for the given inodeId
  public static void createS3Processable(Long inodeId) throws IOException {
    S3ProcessableDataAccess<S3Processable> da = getProcessableDataAccess();
    LightWeightRequestHandler handler = new LightWeightRequestHandler(HDFSOperationType.CREATE_S3_PROCESSABLE) {
      @Override
      public Object performTask() throws IOException {
        S3Processable processable = da.findByInodeId(inodeId);
        if(processable == null) {
          da.add(new S3Processable(inodeId, null, null));
        }
        return null;
      }
    };
    handler.handle();
  }

  private long getLeaderCheckPeriod() {
    return nn.getLeaderElectionInstance().getCurrentTimePeriod();
  }

  public synchronized void stopThread() throws InterruptedException {
    this.interrupt();
    running = false;
  }

  private void stopInternal() throws InterruptedException {
    stopLeaders();
    stopWorkers();
  }

  private void stopWorkers() throws InterruptedException {
    if(deleteWorker != null) {
      LOG.info("Stopping delete worker...");
      deleteWorker.stopThread();
    } else {
      LOG.info("No delete worker to stop...");
    }
    if(processableWorker != null) {
      LOG.info("Stopping processable worker...");
      processableWorker.stopThread();
    } else {
      LOG.info("No processable worker to stop...");
    }

    if(deleteWorker != null) {
      deleteWorker.join();
    }
    if(processableWorker != null) {
      processableWorker.join();
    }
  }

  private void stopLeaders() throws InterruptedException {
    if(deleteLeader != null) {
      LOG.info("Stopping delete leader...");
      deleteLeader.stopThread();
    } else {
      LOG.info("No delete leader to be stop...");
    }
    if(processableLeader != null) {
      LOG.info("Stopping processable leader...");
      processableLeader.stopThread();
    } else {
      LOG.info("No processable leader to stop...");
    }

    if(deleteLeader != null) {
      deleteLeader.join();
    }
    if(processableLeader != null) {
      processableLeader.join();
    }
  }

  class DeleteLeader extends Thread {
    private int lookupInterval;
    private int scheduleTimeout;
    private int fetchSize;
    private boolean running = true;

    DeleteLeader() {
      lookupInterval = conf.getInt(
              DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_OBJECT_MANAGEMENT_DELETION_LEADER_LOOKUP_INTERVAL_KEY,
              DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_OBJECT_MANAGEMENT_DELETION_LEADER_LOOKUP_INTERVAL_DEFAULT);
      scheduleTimeout = conf.getInt(
              DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_OBJECT_MANAGEMENT_DELETION_LEADER_SCHEDULE_TIMEOUT_KEY,
              DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_OBJECT_MANAGEMENT_DELETION_LEADER_SCHEDULE_TIMEOUT_DEFAULT);
      fetchSize = conf.getInt(
              DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_OBJECT_MANAGEMENT_DELETION_LEADER_FETCH_SIZE_KEY,
              DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_OBJECT_MANAGEMENT_DELETION_LEADER_FETCH_SIZE_DEFAULT);
    }

    @Override
    public void run() {
      while(running) {
        try {
          checkAvailableAndSchedule();
        } catch (IOException e) {
          LOG.warn("Failed to check and schedule available S3 deletables.", e);
        }
        try {
          Thread.sleep(lookupInterval);
        } catch (InterruptedException e) {
          LOG.info("DeleteLeader: Thread interrupted; terminating loop.");
          running = false;
        }
      }
    }

    public void stopThread() {
      this.interrupt();
      running = false;
    }

    private void checkAvailableAndSchedule() throws IOException {
      LightWeightRequestHandler handler = new LightWeightRequestHandler(HDFSOperationType.SCHEDULE_S3_DELETABLES) {
        @Override
        public Object performTask() throws IOException {
          List<S3ObjectDeletable> deletables = getAvailableForScheduling();
          if(deletables.size() > 0) {
            scheduleDeletables(deletables);
          }
          return null;
        }
      };
      handler.handle();
    }

    private void scheduleDeletables(List<S3ObjectDeletable> deletables) throws IOException {
      SortedActiveNodeList sortedActiveNodes = nn.getActiveNameNodes();
      int idxActiveNode = 0;
      Long rescheduleAt = Time.now() + scheduleTimeout;
      for(S3ObjectDeletable deletable : deletables) {
        ActiveNode currentNode = sortedActiveNodes.getActiveNodes().get(idxActiveNode);
        deletable.setRescheduleAt(rescheduleAt);
        // TODO FCG: I do not think locking this part is necessary. Failure would just require another checking cycle
        //  and nothing would be lost.
        // TODO FCG: Refactor this messy way of getting the node address.
        InetSocketAddress inetAddr = currentNode.getRpcServerAddressForClients();
        String addr = inetAddr.getHostName() +  ":" + inetAddr.getPort();
        deletable.setScheduledFor(addr);

        idxActiveNode++;
        idxActiveNode %= sortedActiveNodes.size();
      }
      getDeletableDataAccess().updateAll(deletables);
    }

    private List<S3ObjectDeletable> getAvailableForScheduling() throws IOException {
      long currentTimestamp = Time.now();
      return getDeletableDataAccess().getNFirstAvailableForScheduling(fetchSize, currentTimestamp);
    }
  }

  class DeleteWorker extends Thread {
    private int lookupInterval;
    private boolean running = true;

    DeleteWorker() {
      lookupInterval = conf.getInt(
              DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_OBJECT_MANAGEMENT_DELETION_WORKER_LOOKUP_INTERVAL_KEY,
              DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_OBJECT_MANAGEMENT_DELETION_WORKER_LOOKUP_INTERVAL_DEFAULT);
    }

    @Override
    public void run() {
      while(running) {
        try {
          checkAndExecuteScheduled();
        } catch (IOException e) {
          LOG.warn("Failed to check and execute scheduled deletables.", e);
        }
        try {
          Thread.sleep(lookupInterval);
        } catch (InterruptedException e) {
          LOG.info("DeleteWorker: Thread interrupted; terminating loop.");
          running = false;
        }
      }
    }

    public void stopThread() {
      this.interrupt();
      running = false;
    }

    private void checkAndExecuteScheduled() throws IOException {
      List<S3ObjectDeletable> deletables = getScheduledDeletables();
      if(deletables.size() > 0) {
        deleteScheduled(deletables);
      }
    }

    private List<S3ObjectDeletable> getScheduledDeletables() throws IOException {
      InetSocketAddress inetAddr = nn.getNameNodeAddress();
      String addr = inetAddr.getAddress().getHostAddress() +  ":" + inetAddr.getPort();
      return getDeletableDataAccess().getScheduledFor(addr);
    }

    private void deleteScheduled(List<S3ObjectDeletable> deletables) {
      List<S3ObjectDeletable> successfullyDeletedFromS3 = new ArrayList<>();

      // TODO FCG: Consider the possibility that the object does not exist anymore
      for(S3ObjectDeletable deletable : deletables) {
        DeleteVersionRequest dvr =
                new DeleteVersionRequest(deletable.getBucket(), deletable.getKey(), deletable.getVersionId());
        try {
          s3.deleteVersion(dvr);
        } catch(SdkClientException sce) {
          LOG.warn("Failed to delete from S3: " + deletable.toString());
          continue;
        }

        successfullyDeletedFromS3.add(deletable);
      }

      try {
        removeRecords(successfullyDeletedFromS3);
        LOG.info("Successfully removed deletables: " + successfullyDeletedFromS3);
      } catch (IOException e) {
        LOG.info("Failed to remove deletable records: " + successfullyDeletedFromS3, e);
      }
    }

    private void removeRecords(List<S3ObjectDeletable> deletables) throws IOException {
      getDeletableDataAccess().deleteAll(deletables);
    }
  }

  class ProcessableLeader extends Thread {
    private int lookupInterval;
    private int scheduleTimeout;
    private int fetchSize;
    private boolean running = true;

    ProcessableLeader() {
      lookupInterval = conf.getInt(
              DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_OBJECT_MANAGEMENT_CONSOLIDATION_LEADER_LOOKUP_INTERVAL_KEY,
              DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_OBJECT_MANAGEMENT_CONSOLIDATION_LEADER_LOOKUP_INTERVAL_DEFAULT);
      scheduleTimeout = conf.getInt(
              DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_OBJECT_MANAGEMENT_CONSOLIDATION_LEADER_SCHEDULE_TIMEOUT_KEY,
              DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_OBJECT_MANAGEMENT_CONSOLIDATION_LEADER_SCHEDULE_TIMEOUT_DEFAULT);
      fetchSize = conf.getInt(
              DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_OBJECT_MANAGEMENT_CONSOLIDATION_LEADER_FETCH_SIZE_KEY,
              DFSConfigKeys.DFS_NAMENODE_OBJECT_STORAGE_OBJECT_MANAGEMENT_CONSOLIDATION_LEADER_FETCH_SIZE_DEFAULT);
    }

    @Override
    public void run() {
      while(running) {
        try {
          checkAvailableAndSchedule();
        } catch (IOException e) {
          LOG.warn("Failed to check and schedule available S3 processables.", e);
        }

        try {
          Thread.sleep(lookupInterval);
        } catch (InterruptedException e) {
          LOG.info("ProcessableLeader: Thread interrupted; terminating loop.");
          running = false;
        }
      }
    }

    public void stopThread() {
      this.interrupt();
      running = false;
    }

    // TODO FCG: Limit active nodes to one processable each?
    private void checkAvailableAndSchedule() throws IOException {
      LightWeightRequestHandler handler = new LightWeightRequestHandler(HDFSOperationType.SCHEDULE_S3_PROCESSABLES) {
        @Override
        public Object performTask() throws IOException {
          long currentTimestamp = Time.now();
          List<S3Processable> processables =
                  getProcessableDataAccess().getNFirstAvailableForScheduling(
                          fetchSize, currentTimestamp);

          if(processables.size() == 0) {
            return null;
          }

          LOG.info("About to schedule processables: " + processables);

          SortedActiveNodeList sortedActiveNodes = nn.getActiveNameNodes();
          int idxActiveNode = 0;
          Long rescheduleAt = Time.now() + scheduleTimeout;
          for(S3Processable processable : processables) {
            ActiveNode currentNode = sortedActiveNodes.getActiveNodes().get(idxActiveNode);

            processable.setRescheduleAt(rescheduleAt);

            InetSocketAddress inetAddr = currentNode.getRpcServerAddressForClients();
            String addr = inetAddr.getHostName() +  ":" + inetAddr.getPort();
            processable.setScheduledFor(addr);

            idxActiveNode++;
            idxActiveNode %= sortedActiveNodes.size();
          }

          getProcessableDataAccess().updateAll(processables);
          return null;
        }
      };
      handler.handle();
    }
  }

  class ProcessableWorker extends Thread {
    private int lookupInterval;
    private int bufferSize;
    static final int BUFFER_SIZE = 1_024;
    private boolean running = true;

    ProcessableWorker() {
      lookupInterval = conf.getInt(
              DFS_NAMENODE_OBJECT_STORAGE_OBJECT_MANAGEMENT_CONSOLIDATION_WORKER_LOOKUP_INTERVAL_KEY,
              DFS_NAMENODE_OBJECT_STORAGE_OBJECT_MANAGEMENT_CONSOLIDATION_WORKER_LOOKUP_INTERVAL_DEFAULT);
    }

    @Override
    public void run() {
      while(running) {
        try {
          checkAndExecuteScheduled();
        } catch (IOException e) {
          LOG.warn("Failed to check and execute scheduled S3 processables.", e);
        }
        try {
          Thread.sleep(lookupInterval);
        } catch (InterruptedException e) {
          LOG.info("ProcessableWorker: Thread interrupted; terminating loop.");
          running = false;
        }
      }
    }

    public void stopThread() {
      this.interrupt();
      running = false;
    }

    private void checkAndExecuteScheduled() throws IOException {
      List<S3Processable> processables = getScheduledProcessables();
      if(processables.size() > 0) {
        for(S3Processable processable : processables) {
          processScheduled(processable);
        }
      }
    }

    private void processScheduled(S3Processable processable) throws IOException {
        INode inode = INodeUtil.getNode(processable.getInodeId(), false);
        if(inode.isFile()) {
          LOG.info("About to process file: " + processable);
          processScheduledFile(processable, (INodeFile)inode);
        } else if(inode.isDirectory()) {
          LOG.info("About to process directory: " + processable);
          processScheduledDirectory(processable, (INodeDirectory)inode);
        } else {
          LOG.info("About to process symlink: " + processable);
          // Delete if symlink
          getProcessableDataAccess().delete(processable);
        }
    }

    private void processScheduledDirectory(S3Processable processable, INodeDirectory dir) throws IOException {
      FSNamesystem fsn = nn.getNamesystem();

      AbstractFileTree.FileTree fileTree = (AbstractFileTree.FileTree) (new LightWeightRequestHandler(HDFSOperationType.GET_FILETREE) {
        @Override
        public Object performTask() throws IOException {
          INode iNode = INodeUtil.getNode(dir.getId(), false);
          INodeIdentifier id = new INodeIdentifier(iNode.getId(), iNode.getParentId(), iNode.getLocalName(), iNode.getPartitionId());
          id.setDepth(INodeUtil.findINodeDepthWithNoTransaction(iNode));
          id.setStoragePolicy(INodeUtil.findINodeStoragePolicyWithNoTransaction(iNode));
          return new AbstractFileTree.FileTree(fsn, id);
        }
      }).handle();

      fileTree.buildUp(fsn.getFSDirectory().getBlockStoragePolicySuite());

      INodesInPath iip = INodesInPath.fromINodeNoTransaction(dir);
      String src = iip.getPath();

      for (int i = fileTree.getHeight(); i > 0; i--) {
        createProcessablesForTreeLevel(fsn, src, fileTree.getSubtreeRoot().getId(), fileTree, i);
      }

      getProcessableDataAccess().delete(processable);
    }

    private void createProcessablesForTreeLevel(FSNamesystem fsn, final String subtreeRootPath, final long subTreeRootID,
                                                final AbstractFileTree.FileTree fileTree, int level) throws IOException {
      for(final ProjectedINode dir : fileTree.getDirsByLevel(level)) {
        ArrayList<S3Processable> fileProcessables = new ArrayList<>();
        for(final ProjectedINode inode : fileTree.getChildren(dir.getId())) {
          if(!inode.isDirectory() && !inode.isSymlink()) {
            fileProcessables.add(new S3Processable(inode.getId()));
          }
        }
        getProcessableDataAccess().updateAll(fileProcessables);
      }
    }

    private void processScheduledFile(S3Processable processable, INodeFile file) throws IOException {
      List<S3ObjectInfoContiguous> objs = getObjectDataAccess().findByInodeId(processable.getInodeId());
      objs.sort(S3ObjectInfoContiguous.Order.ByS3ObjectIndex);

      LOG.info("About to process processable: " + processable);

      String finalKey = FSDirectory.getFullPathNameNoTransaction(file);
      S3File s3File = new S3File();
      s3File.setSubclassObjects(objs);

      S3UploadOutputStream s3UploadOutputStream = new S3UploadOutputStream(s3, conf, finalKey, threadPoolExecutor);
      byte[] buf = new byte[BUFFER_SIZE];
      long fullSize = 0;
      for(RangedS3Object rangedS3Object : s3File.getObjectsInFullRange()) {
        fullSize += rangedS3Object.getNumBytes();
      }

      HopsTransactionalRequestHandler handler;

      if(fullSize > 5 * TERABYTE) {
        handler = new HopsTransactionalRequestHandler(HDFSOperationType.REPLACE_S3_OBJECTS) {
          @Override
          public void acquireLock(TransactionLocks locks) {
            LockFactory lf = LockFactory.getInstance();
            INodeIdentifier iNodeIdentifier = new INodeIdentifier(processable.getInodeId());
            locks.add(lf.getIndividualINodeLock(TransactionLockTypes.INodeLockType.WRITE, iNodeIdentifier))
                    .add(lf.getS3ObjectLock());
          }

          @Override
          public Object performTask() throws IOException {
            S3ProcessableDataAccess<S3Processable> procDa = getProcessableDataAccess();
            procDa.delete(processable);
            LOG.info("Skipping processable because it's larger than 5TB (S3 limit): " + processable);
            return null;
          }
        };
        handler.handle();
      }

      for(RangedS3Object rangedS3Object : s3File.getObjectsInFullRange()) {
        fullSize += rangedS3Object.getNumBytes();
        S3DownloadInputStream s3DownloadInputStream = new S3DownloadInputStream(s3, conf, threadPoolExecutor, rangedS3Object);
        IOUtils.copyLarge(s3DownloadInputStream, s3UploadOutputStream, buf);
      }
      s3UploadOutputStream.close();

      final S3ObjectInfoContiguous fullObject = nn.getNamesystem().createS3Object(
              processable.getInodeId(), finalKey, s3UploadOutputStream.getVersionId(), fullSize, 0, false);

      final long processableSize = fullSize;
      handler = new HopsTransactionalRequestHandler(HDFSOperationType.REPLACE_S3_OBJECTS) {
        @Override
        public void acquireLock(TransactionLocks locks) {
          LockFactory lf = LockFactory.getInstance();
          INodeIdentifier iNodeIdentifier = new INodeIdentifier(processable.getInodeId());
          locks.add(lf.getIndividualINodeLock(TransactionLockTypes.INodeLockType.WRITE, iNodeIdentifier))
                  .add(lf.getS3ObjectLock());
        }

        @Override
        public Object performTask() throws IOException {
          S3ObjectInfoDataAccess<S3ObjectInfoContiguous> da = getObjectDataAccess();
          List<S3ObjectInfoContiguous> currentObjects = da.findByInodeId(processable.getInodeId());
          long currentSize = 0;
          for(S3ObjectInfoContiguous obj : currentObjects) {
            currentSize += obj.getNumBytes();
          }

          if(currentSize != processableSize) {
            LOG.info("Aborting processable " + processable + " because the current state of the inode is different from" +
                    " that on which the processing was based: current size is different from processed size.");
            return null;
          }

          da.deleteAll(currentObjects);
          da.add(fullObject);

          S3ProcessableDataAccess<S3Processable> procDa = getProcessableDataAccess();
          procDa.delete(processable);
          LOG.info("Completed processable: " + processable);
          return null;
        }
      };
      handler.handle();
    }

    private List<S3Processable> getScheduledProcessables() throws IOException {
      InetSocketAddress inetAddr = nn.getNameNodeAddress();
      String addr = inetAddr.getAddress().getHostAddress() +  ":" + inetAddr.getPort();
      return getProcessableDataAccess().getScheduledFor(addr);
    }
  }
}
