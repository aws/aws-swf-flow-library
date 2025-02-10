package com.amazonaws.services.simpleworkflow.flow.hierarchyaffinity;

import com.amazonaws.services.simpleworkflow.flow.ChildWorkflowIdHandler;
import com.amazonaws.services.simpleworkflow.flow.model.WorkflowExecution;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

public class HierarchyAffinityChildWorkflowIdHandler implements ChildWorkflowIdHandler {
    private static final String AFFINITY_PREFIX = "A000";
    private static final String AFFINITY_PREFIX_WITH_DELIMITER = "A000$";
    private static final Pattern DELIMITER_PATTERN = Pattern.compile("\\$");

    @Override
    public String generateWorkflowId(WorkflowExecution currentWorkflow, Supplier<String> nextId) {
        return String.format("%s%s:%s", AFFINITY_PREFIX_WITH_DELIMITER, currentWorkflow.getRunId(), nextId.get());
    }

    @Override
    public String extractRequestedWorkflowId(String childWorkflowId) {
        if (!childWorkflowId.startsWith(AFFINITY_PREFIX_WITH_DELIMITER)) {
            return childWorkflowId;
        }

        final String[] parts = DELIMITER_PATTERN.split(childWorkflowId);
        if (parts.length != 5) {
            return childWorkflowId;
        }

        final String childId = parts[1];
        final String rootId = parts[2];
        final String shardCountStr = parts[3];
        final String checksum = parts[4];

        final CRC32 crc = new CRC32();
        crc.update(AFFINITY_PREFIX.getBytes(StandardCharsets.UTF_8));
        crc.update(childId.getBytes(StandardCharsets.UTF_8));
        crc.update(rootId.getBytes(StandardCharsets.UTF_8));
        crc.update(shardCountStr.getBytes(StandardCharsets.UTF_8));

        if (!checksum.equals(Long.toHexString(crc.getValue()))) {
            return childWorkflowId;
        }

        return AFFINITY_PREFIX_WITH_DELIMITER + childId;
    }
}
