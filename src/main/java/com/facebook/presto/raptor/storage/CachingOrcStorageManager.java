/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.raptor.storage;

import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.raptor.RaptorConnectorId;
import com.facebook.presto.raptor.backup.BackupManager;
import com.facebook.presto.raptor.backup.BackupStore;
import com.facebook.presto.raptor.metadata.ShardRecorder;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_RECOVERY_ERROR;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_RECOVERY_TIMEOUT;
import static com.google.common.base.Throwables.propagateIfInstanceOf;

public class CachingOrcStorageManager
        extends OrcStorageManager
{
    private final StorageService storageService;
    private final Optional<BackupStore> backupStore;
    private final ShardRecoveryManager recoveryManager;
    private final Duration recoveryTimeout;

    @Inject
    public CachingOrcStorageManager(NodeManager nodeManager, StorageService storageService, Optional<BackupStore> backupStore, ReaderAttributes readerAttributes, StorageManagerConfig config, RaptorConnectorId connectorId, BackupManager backgroundBackupManager, ShardRecoveryManager recoveryManager, ShardRecorder shardRecorder, TypeManager typeManager)
    {
        super(nodeManager, storageService, backupStore, readerAttributes, config, connectorId, backgroundBackupManager, recoveryManager, shardRecorder, typeManager);
        this.storageService = storageService;
        this.backupStore = backupStore;
        this.recoveryManager = recoveryManager;
        this.recoveryTimeout = config.getShardRecoveryTimeout();
    }

    @Override
    OrcDataSource openShard(UUID shardUuid, ReaderAttributes readerAttributes)
    {
        File file = storageService.getStorageFile(shardUuid).getAbsoluteFile();

        if (!file.exists() && backupStore.isPresent()) {
            try {
                Future<?> future = recoveryManager.recoverShard(shardUuid);
                future.get(recoveryTimeout.toMillis(), TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
            catch (ExecutionException e) {
                propagateIfInstanceOf(e.getCause(), PrestoException.class);
                throw new PrestoException(RAPTOR_RECOVERY_ERROR, "Error recovering shard " + shardUuid, e.getCause());
            }
            catch (TimeoutException e) {
                throw new PrestoException(RAPTOR_RECOVERY_TIMEOUT, "Shard is being recovered from backup. Please retry in a few minutes: " + shardUuid);
            }
        }

        try {
            return memoryMappedOrcDataSource(readerAttributes, file);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to open shard file: " + file, e);
        }
    }

    private static OrcDataSource memoryMappedOrcDataSource(ReaderAttributes readerAttributes, File file)
            throws FileNotFoundException
    {
        return new MemoryOrcDataSource(file, readerAttributes.getMaxMergeDistance(), readerAttributes.getMaxReadSize(), readerAttributes.getStreamBufferSize());
    }
}
