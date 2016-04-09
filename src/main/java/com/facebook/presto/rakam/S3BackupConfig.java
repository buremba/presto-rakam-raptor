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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import io.airlift.configuration.Config;

public class S3BackupConfig
{
    private String accessKey;
    private String secretAccessKey;
    private String s3Bucket;
    private String region;
    private String endpoint;

    @Config("aws.access-key")
    public S3BackupConfig setAccessKey(String accessKey)
    {
        this.accessKey = accessKey;
        return this;
    }

    @Config("aws.s3-bucket")
    public S3BackupConfig setS3Bucket(String s3Bucket)
    {
        this.s3Bucket = s3Bucket;
        return this;
    }

    public String getRegion()
    {
        return region;
    }

    public Region getAWSRegion()
    {
        return Region.getRegion(region == null ? Regions.DEFAULT_REGION : Regions.fromName(region));
    }

    @Config("aws.region")
    public S3BackupConfig setRegion(String region)
    {
        this.region = region;
        return this;
    }

    public String getS3Bucket()
    {
        return s3Bucket;
    }

    public String getAccessKey()
    {
        return accessKey;
    }

    @Config("aws.secret-access-key")
    public S3BackupConfig setSecretAccessKey(String secretAccessKey)
    {
        this.secretAccessKey = secretAccessKey;
        return this;
    }

    public String getSecretAccessKey()
    {
        return secretAccessKey;
    }

    @Config("aws.s3-endpoint")
    public S3BackupConfig setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    public String getEndpoint()
    {
        return endpoint;
    }

    public AWSCredentialsProvider getCredentials()
    {
        if (accessKey == null && secretAccessKey == null) {
            return new InstanceProfileCredentialsProvider();
        }
        return new StaticCredentialsProvider(new BasicAWSCredentials(getAccessKey(), getSecretAccessKey()));
    }
}
