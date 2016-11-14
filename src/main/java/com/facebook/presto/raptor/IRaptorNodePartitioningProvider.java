package com.facebook.presto.raptor;

import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.raptor.util.Types.checkType;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.google.common.collect.Maps.uniqueIndex;

public class IRaptorNodePartitioningProvider
        extends RaptorNodePartitioningProvider
{
    private final NodeSupplier nodeSupplier;

    @Inject
    public IRaptorNodePartitioningProvider(NodeSupplier nodeSupplier)
    {
        super(nodeSupplier);
        this.nodeSupplier = nodeSupplier;
    }

    @Override
    public BucketFunction getBucketFunction(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorPartitioningHandle partitioning, List<Type> partitionChannelTypes, int bucketCount)
    {
        IRaptorPartitioningHandle handle = checkType(partitioning, IRaptorPartitioningHandle.class, "partitioningHandle");
        return new IRaptorBucketFunction(bucketCount, handle.getBucketTypes());
    }

    @Override
    public Map<Integer, Node> getBucketToNode(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorPartitioningHandle partitioning)
    {
        IRaptorPartitioningHandle handle = checkType(partitioning, IRaptorPartitioningHandle.class, "distributionHandle");

        Map<String, Node> nodesById = uniqueIndex(nodeSupplier.getWorkerNodes(), Node::getNodeIdentifier);

        ImmutableMap.Builder<Integer, Node> bucketToNode = ImmutableMap.builder();
        for (Map.Entry<Integer, String> entry : handle.getBucketToNode().entrySet()) {
            Node node = nodesById.get(entry.getValue());
            if (node == null) {
                throw new PrestoException(NO_NODES_AVAILABLE, "Node for bucket is offline: " + entry.getValue());
            }
            bucketToNode.put(entry.getKey(), node);
        }
        return bucketToNode.build();
    }
}
