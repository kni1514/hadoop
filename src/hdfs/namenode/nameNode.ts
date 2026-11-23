import { HDFSNamespace } from './namespace';
import { BlockPlacementPolicyDefault } from './placementPolicy';
import { Block, BlockId, INodeFile, NodeId, Replica, VirtualNode, NameNodeState } from '../../core/domain/types';
import { transitionNameNode } from '../../core/fsm/stateMachines';
import { SimulationEngine } from '../../core/simulation/engine';
import { VirtualStorageEngine } from '../datanode/storageEngine';

export interface FSCKReport {
  totalFiles: number;
  totalBlocks: number;
  totalReplicas: number;
  underReplicatedBlocks: number;
  corruptBlocks: number;
  missingBlocks: number;
  isHealthy: boolean;
  details: string[];
}

export class NameNode {
  private state: NameNodeState = 'STARTING';
  private namespace: HDFSNamespace;
  private placementPolicy: BlockPlacementPolicyDefault;
  private blockMap: Map<BlockId, Block> = new Map();
  private fileContentMap: Map<string, string> = new Map();
  private dataNodes: Map<NodeId, VirtualNode> = new Map();
  private engine: SimulationEngine;
  private storageEngine: VirtualStorageEngine;
  private heartbeatTimeoutMs = 10000;

  constructor(engine: SimulationEngine) {
    this.engine = engine;
    this.namespace = new HDFSNamespace();
    this.placementPolicy = new BlockPlacementPolicyDefault();
    this.storageEngine = new VirtualStorageEngine();

    this.state = transitionNameNode('STARTING', 'SAFE_MODE');

    this.engine.scheduleEvent('NAMENODE_ENTER_SAFEMODE', 'HDFS', 'NameNode', 0, () => {});

    this.engine.scheduleEvent('NAMENODE_LEAVE_SAFEMODE', 'HDFS', 'NameNode', 1000, () => {
      this.state = transitionNameNode('SAFE_MODE', 'ACTIVE');
    });

    this.scheduleHeartbeatAudit();
  }

  public getState(): NameNodeState {
    return this.state;
  }

  public getNamespace(): HDFSNamespace {
    return this.namespace;
  }

  public registerDataNode(node: VirtualNode): void {
    this.dataNodes.set(node.id, node);
    this.engine.getEventStore().record(
      this.engine.getEventStore().createEvent(
        'DATANODE_REGISTERED',
        'HDFS',
        node.id,
        { rackId: node.rackId, capacity: node.storageCapacityBytes },
        this.engine.getClock().getTime()
      )
    );
  }

  public getDataNodes(): VirtualNode[] {
    return Array.from(this.dataNodes.values());
  }

  public receiveHeartbeat(nodeId: NodeId, usedBytes: number, blockIds: BlockId[]): void {
    const dn = this.dataNodes.get(nodeId);
    if (!dn) return;

    dn.lastHeartbeatTime = this.engine.getClock().getTime();
    dn.storageUsedBytes = usedBytes;
    dn.blocks = [...blockIds];

    this.engine.getEventStore().record(
      this.engine.getEventStore().createEvent(
        'DATANODE_HEARTBEAT',
        'HDFS',
        nodeId,
        { usedBytes, blockCount: blockIds.length },
        this.engine.getClock().getTime()
      )
    );
  }

  public createAndWriteFile(
    pathStr: string,
    content: string | Uint8Array,
    replicationFactor: number = 3,
    blockSizeBytes: number = 134217728,
    writerNodeId?: NodeId
  ): INodeFile {
    const encoder = new TextEncoder();
    const data = typeof content === 'string' ? encoder.encode(content) : content;
    const textContent = typeof content === 'string' ? content : new TextDecoder().decode(content);
    const totalSize = data.length;

    // Overwriting a file must release the old file's blocks first, otherwise the
    // DataNodes keep counting space for data that no longer exists.
    const existing = this.namespace.resolvePath(pathStr);
    if (existing && existing.type === 'FILE') {
      this.releaseFileBlocks(existing);
    }

    const file = this.namespace.createFile(pathStr, totalSize, replicationFactor, blockSizeBytes);
    this.fileContentMap.set(file.path, textContent);

    const numBlocks = Math.max(1, Math.ceil(totalSize / blockSizeBytes));
    const allDataNodes = this.getDataNodes();

    for (let i = 0; i < numBlocks; i++) {
      const blockId = `blk_${Date.now()}_${Math.floor(Math.random() * 100000)}`;
      const start = i * blockSizeBytes;
      const end = Math.min(start + blockSizeBytes, totalSize);
      const blockChunk = data.slice(start, end);
      const checksum = this.storageEngine.calculateChecksum(blockChunk);

      const targets = this.placementPolicy.chooseTargets(allDataNodes, writerNodeId, replicationFactor);

      const replicas: Replica[] = targets.map((target) => ({
        blockId,
        nodeId: target.id,
        rackId: target.rackId,
        state: 'FINALIZED',
        storagePath: `/data/${blockId}`,
        updatedAt: this.engine.getClock().getTime()
      }));

      const block: Block = {
        id: blockId,
        filePath: file.path,
        blockIndex: i,
        sizeBytes: blockChunk.length,
        checksum,
        checksumStatus: 'VALID',
        generationStamp: 1000 + i,
        replicas,
        state: 'FINALIZED'
      };

      this.blockMap.set(blockId, block);
      file.blocks.push(blockId);

      targets.forEach((targetNode) => {
        targetNode.blocks.push(blockId);
        targetNode.storageUsedBytes += blockChunk.length;
        this.storageEngine.writeBlockData(blockId, blockChunk);
      });

      this.engine.getEventStore().record(
        this.engine.getEventStore().createEvent(
          'BLOCK_CREATED',
          'HDFS',
          'NameNode',
          { blockId, filePath: file.path, replicas: targets.map((t) => t.id) },
          this.engine.getClock().getTime()
        )
      );
    }

    return file;
  }

  /** Free every block of a file: block map, DataNode block lists, used bytes and stored bytes. */
  private releaseFileBlocks(file: INodeFile): void {
    for (const blockId of file.blocks) {
      const block = this.blockMap.get(blockId);
      if (block) {
        for (const replica of block.replicas) {
          const dn = this.dataNodes.get(replica.nodeId);
          if (dn) {
            dn.blocks = dn.blocks.filter((b) => b !== blockId);
            dn.storageUsedBytes = Math.max(0, dn.storageUsedBytes - block.sizeBytes);
          }
        }
      }
      this.blockMap.delete(blockId);
      this.storageEngine.deleteBlockData(blockId);
    }
    this.fileContentMap.delete(file.path);
  }

  /** All files under a path (the path itself when it is a file). */
  public listFilesUnder(pathStr: string): INodeFile[] {
    const node = this.namespace.resolvePath(pathStr);
    const files: INodeFile[] = [];
    const walk = (n: any) => {
      if (!n) return;
      if (n.type === 'FILE') files.push(n);
      else n.children.forEach((c: any) => walk(c));
    };
    walk(node);
    return files;
  }

  /**
   * Delete a file or directory and release its storage.
   * Returns false when the path does not exist; throws for a non-empty directory without recursive.
   */
  public deletePath(pathStr: string, recursive: boolean = false): boolean {
    const node = this.namespace.resolvePath(pathStr);
    if (!node) return false;
    if (node.type === 'DIRECTORY' && node.children.size > 0 && !recursive) {
      throw new Error(`Directory is not empty: ${this.namespace.normalizePath(pathStr)}`);
    }
    const files = this.listFilesUnder(pathStr);
    const ok = this.namespace.deletePath(pathStr, recursive);
    if (ok) files.forEach((f) => this.releaseFileBlocks(f));
    return ok;
  }

  /** Rename/move a file or directory inside HDFS (metadata only, like real HDFS). */
  public renamePath(srcPath: string, dstPath: string): void {
    const ns = this.namespace;
    const src = ns.normalizePath(srcPath);
    const dst = ns.normalizePath(dstPath);
    if (src === '/') throw new Error('Cannot rename the root directory');
    if (dst === src || dst.startsWith(src + '/')) throw new Error(`Cannot move '${src}' into itself`);
    const node = ns.resolvePath(src);
    if (!node) throw new Error(`\`${src}': No such file or directory`);
    if (ns.resolvePath(dst)) throw new Error(`\`${dst}': File exists`);
    const dstParentPath = dst.substring(0, dst.lastIndexOf('/')) || '/';
    const dstParent = ns.resolvePath(dstParentPath);
    if (!dstParent || dstParent.type !== 'DIRECTORY') throw new Error(`\`${dstParentPath}': No such file or directory`);
    const srcParent = ns.resolvePath(src.substring(0, src.lastIndexOf('/')) || '/') as any;

    const oldFiles = this.listFilesUnder(src).map((f) => f.path);
    srcParent.children.delete(node.name);
    node.name = dst.substring(dst.lastIndexOf('/') + 1);
    (dstParent as any).children.set(node.name, node);
    const repath = (n: any, p: string) => {
      n.path = p;
      if (n.type === 'DIRECTORY') n.children.forEach((c: any) => repath(c, (p === '/' ? '' : p) + '/' + c.name));
    };
    repath(node, dst);
    const now = Date.now();
    srcParent.modifiedAt = now;
    (dstParent as any).modifiedAt = now;

    for (const oldPath of oldFiles) {
      const newPath = dst + oldPath.substring(src.length);
      const content = this.fileContentMap.get(oldPath);
      this.fileContentMap.delete(oldPath);
      if (content !== undefined) this.fileContentMap.set(newPath, content);
      const file = ns.resolvePath(newPath) as INodeFile | undefined;
      file?.blocks.forEach((b) => {
        const block = this.blockMap.get(b);
        if (block) block.filePath = newPath;
      });
    }
  }

  /** Change a file's replication factor and add/remove replicas to match. */
  public setReplication(pathStr: string, replication: number): void {
    const file = this.namespace.resolvePath(pathStr);
    if (!file || file.type !== 'FILE') throw new Error(`\`${pathStr}': No such file`);
    const target = Math.max(1, Math.min(replication, 512));
    file.replicationFactor = target;
    const liveNodes = this.getDataNodes().filter((n) => n.state === 'RUNNING');
    for (const blockId of file.blocks) {
      const block = this.blockMap.get(blockId);
      if (!block) continue;
      while (block.replicas.length > target) {
        const removed = block.replicas.pop()!;
        const dn = this.dataNodes.get(removed.nodeId);
        if (dn) {
          dn.blocks = dn.blocks.filter((b) => b !== blockId);
          dn.storageUsedBytes = Math.max(0, dn.storageUsedBytes - block.sizeBytes);
        }
      }
      if (block.replicas.length < target) {
        const existing = new Set(block.replicas.map((r) => r.nodeId));
        const extra = this.placementPolicy.chooseTargets(liveNodes, undefined, target - block.replicas.length, existing);
        extra.forEach((dn) => {
          block.replicas.push({ blockId, nodeId: dn.id, rackId: dn.rackId, state: 'FINALIZED', storagePath: `/data/${blockId}`, updatedAt: this.engine.getClock().getTime() });
          dn.blocks.push(blockId);
          dn.storageUsedBytes += block.sizeBytes;
        });
      }
    }
  }

  /** Erase all HDFS data (used by 'hdfs namenode -format'). */
  public formatNamespace(): void {
    this.listFilesUnder('/').forEach((f) => this.releaseFileBlocks(f));
    const root = this.namespace.getRoot();
    root.children.clear();
    root.modifiedAt = Date.now();
    this.fileContentMap.clear();
  }

  public getBlocksOf(pathStr: string): Block[] {
    const file = this.namespace.resolvePath(pathStr);
    if (!file || file.type !== 'FILE') return [];
    return file.blocks.map((b) => this.blockMap.get(b)).filter(Boolean) as Block[];
  }

  public readFileContent(pathStr: string): string | undefined {
    const norm = this.namespace.normalizePath(pathStr);
    return this.fileContentMap.get(norm);
  }

  public getBlock(blockId: BlockId): Block | undefined {
    return this.blockMap.get(blockId);
  }

  public runFSCK(pathStr: string = '/'): FSCKReport {
    const report: FSCKReport = {
      totalFiles: 0,
      totalBlocks: 0,
      totalReplicas: 0,
      underReplicatedBlocks: 0,
      corruptBlocks: 0,
      missingBlocks: 0,
      isHealthy: true,
      details: []
    };

    const targetNode = this.namespace.resolvePath(pathStr);
    if (!targetNode) {
      report.isHealthy = false;
      report.details.push(`Path not found: ${pathStr}`);
      return report;
    }

    const filesToScan: INodeFile[] = [];
    const collectFiles = (node: any) => {
      if (node.type === 'FILE') {
        filesToScan.push(node);
      } else if (node.type === 'DIRECTORY') {
        node.children.forEach((child: any) => collectFiles(child));
      }
    };
    collectFiles(targetNode);

    report.totalFiles = filesToScan.length;

    for (const file of filesToScan) {
      for (const blockId of file.blocks) {
        report.totalBlocks++;
        const block = this.blockMap.get(blockId);

        if (!block) {
          report.missingBlocks++;
          report.details.push(`Missing metadata for block ${blockId} in file ${file.path}`);
          continue;
        }

        const liveReplicas = block.replicas.filter((r) => {
          const dn = this.dataNodes.get(r.nodeId);
          return dn && dn.state === 'RUNNING';
        });

        report.totalReplicas += liveReplicas.length;

        if (block.checksumStatus === 'CORRUPTED') {
          report.corruptBlocks++;
          report.details.push(`CORRUPT block ${blockId} in file ${file.path}`);
        }

        if (liveReplicas.length === 0) {
          report.missingBlocks++;
          report.details.push(`MISSING block ${blockId} (0 live replicas) in file ${file.path}`);
        } else if (liveReplicas.length < file.replicationFactor) {
          report.underReplicatedBlocks++;
          report.details.push(
            `Under-replicated block ${blockId} in file ${file.path}. Expected: ${file.replicationFactor}, Found: ${liveReplicas.length}`
          );
        }
      }
    }

    report.isHealthy = report.corruptBlocks === 0 && report.missingBlocks === 0 && report.underReplicatedBlocks === 0;
    return report;
  }

  public triggerReReplication(): number {
    let reReplicatedCount = 0;
    const allDataNodes = this.getDataNodes();

    this.blockMap.forEach((block) => {
      const liveReplicas = block.replicas.filter((r) => {
        const dn = this.dataNodes.get(r.nodeId);
        return dn && dn.state === 'RUNNING';
      });

      const fileINode = this.namespace.resolvePath(block.filePath) as INodeFile;
      const targetRepFactor = fileINode ? fileINode.replicationFactor : 3;

      if (liveReplicas.length > 0 && liveReplicas.length < targetRepFactor) {
        const existingNodeIds = new Set(liveReplicas.map((r) => r.nodeId));
        const needed = targetRepFactor - liveReplicas.length;
        const newTargets = this.placementPolicy.chooseTargets(allDataNodes, undefined, needed, existingNodeIds);

        newTargets.forEach((target) => {
          const newReplica: Replica = {
            blockId: block.id,
            nodeId: target.id,
            rackId: target.rackId,
            state: 'FINALIZED',
            storagePath: `/data/${block.id}`,
            updatedAt: this.engine.getClock().getTime()
          };
          block.replicas.push(newReplica);
          target.blocks.push(block.id);
          reReplicatedCount++;

          this.engine.getEventStore().record(
            this.engine.getEventStore().createEvent(
              'BLOCK_REPLICATED',
              'HDFS',
              'NameNode',
              { blockId: block.id, targetNodeId: target.id },
              this.engine.getClock().getTime()
            )
          );
        });
      }
    });

    return reReplicatedCount;
  }

  private scheduleHeartbeatAudit(): void {
    this.engine.scheduleEvent('HEARTBEAT_AUDIT', 'HDFS', 'NameNode', 5000, () => {
      const now = this.engine.getClock().getTime();
      let failedNodesCount = 0;

      this.dataNodes.forEach((dn) => {
        if (dn.state === 'RUNNING' && now - dn.lastHeartbeatTime > this.heartbeatTimeoutMs) {
          dn.state = 'DEAD';
          failedNodesCount++;
          this.engine.getEventStore().record(
            this.engine.getEventStore().createEvent(
              'DATANODE_FAILED',
              'FAILURE',
              dn.id,
              { reason: 'Heartbeat timeout' },
              now
            )
          );
        }
      });

      if (failedNodesCount > 0) {
        this.triggerReReplication();
      }

      this.scheduleHeartbeatAudit();
    });
  }
}
