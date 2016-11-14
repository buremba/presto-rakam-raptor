package com.facebook.presto.raptor;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;

public class IRaptorHandleResolver extends RaptorHandleResolver
{
    @Override
    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass()
    {
        return IRaptorTableLayoutHandle.class;
    }

    @Override
    public Class<? extends ConnectorPartitioningHandle> getPartitioningHandleClass()
    {
        return IRaptorPartitioningHandle.class;
    }
}
