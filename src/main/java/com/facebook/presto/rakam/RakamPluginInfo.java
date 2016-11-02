package com.facebook.presto.rakam;

import com.facebook.presto.raptor.PluginInfo;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;

import java.util.Map;

public class RakamPluginInfo
        extends PluginInfo
{
    @Override
    public String getName()
    {
        return "rakam_raptor";
    }

    public Map<String, Module> getBackupProviders()
    {
        return ImmutableMap.of("s3", new S3BackupStoreModule());
    }
}
