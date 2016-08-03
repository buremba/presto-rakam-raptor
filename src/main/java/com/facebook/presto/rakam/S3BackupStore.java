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
package com.facebook.presto.rakam;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.facebook.presto.raptor.backup.BackupStore;
import com.google.common.base.Throwables;
import org.apache.http.HttpStatus;

import javax.inject.Inject;

import java.util.UUID;

public class S3BackupStore
        implements BackupStore
{
    private final AmazonS3Client s3Client;
    private final S3BackupConfig config;

    @Inject
    public S3BackupStore(S3BackupConfig config)
    {
        this.config = config;
        s3Client = new AmazonS3Client(config.getCredentials());
        if (config.getEndpoint() != null) {
            s3Client.setEndpoint(config.getEndpoint());
        }
        s3Client.setRegion(config.getAWSRegion());
    }

    @Override
    public void backupShard(java.util.UUID uuid, java.io.File source)
    {
        s3Client.putObject(config.getS3Bucket(), uuid.toString(), source);
    }

    @Override
    public void restoreShard(java.util.UUID uuid, java.io.File target)
    {
        try {
            new TransferManager(s3Client).download(config.getS3Bucket(), uuid.toString(), target).waitForCompletion();
        }
        catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public boolean deleteShard(UUID uuid)
    {
        s3Client.deleteObject(config.getS3Bucket(), uuid.toString());
        return true;
    }

    @Override
    public boolean shardExists(java.util.UUID uuid)
    {
        try {
            s3Client.getObjectMetadata(config.getS3Bucket(), uuid.toString());
            return true;
        }
        catch (AmazonS3Exception e) {
            if (e.getStatusCode() == HttpStatus.SC_NOT_FOUND) {
                return false;
            }
            else {
                throw e;
            }
        }
    }
}
