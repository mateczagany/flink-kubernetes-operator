/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.operator.reconciler.snapshot;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.kubernetes.operator.api.CrdConstants;
import org.apache.flink.kubernetes.operator.api.FlinkDeployment;
import org.apache.flink.kubernetes.operator.api.spec.FlinkStateSnapshotSpec;
import org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotState;
import org.apache.flink.kubernetes.operator.api.status.SnapshotTriggerType;
import org.apache.flink.kubernetes.operator.controller.FlinkResourceContext;
import org.apache.flink.kubernetes.operator.controller.FlinkStateSnapshotContext;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.service.FlinkResourceContextFactory;
import org.apache.flink.util.Preconditions;

import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

/** The reconciler for the {@link org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot}. */
@RequiredArgsConstructor
public class StateSnapshotReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(StateSnapshotReconciler.class);
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX");

    private final FlinkResourceContextFactory ctxFactory;

    public void reconcile(FlinkStateSnapshotContext ctx) {
        var source = ctx.getResource().getSpec().getJobReference();
        var resource = ctx.getResource();

        var savepointState = resource.getStatus().getState();
        if (!FlinkStateSnapshotState.TRIGGER_PENDING.equals(savepointState)) {
            return;
        }

        if (resource.getSpec().isSavepoint()
                && resource.getSpec().getSavepoint().isAlreadyExists()) {
            LOG.info(
                    "Snapshot {} is marked as completed in spec, not going to trigger savepoint.",
                    resource.getMetadata().getName());
            resource.getStatus().setState(FlinkStateSnapshotState.COMPLETED);
            resource.getStatus().setPath(resource.getSpec().getSavepoint().getPath());
            return;
        }

        var secondaryResource =
                ctx.getSecondaryResource()
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                String.format(
                                                        "Secondary resource %s not found",
                                                        source)));
        if (!ReconciliationUtils.isJobRunning(secondaryResource.getStatus())) {
            LOG.warn(
                    "Target job {} for savepoint {} is not running, cannot trigger savepoint.",
                    secondaryResource.getMetadata().getName(),
                    resource.getMetadata().getName());
            return;
        }

        var jobId = secondaryResource.getStatus().getJobStatus().getJobId();
        var ctxFlinkDeployment =
                ctxFactory.getResourceContext(
                        ctx.getReferencedJobFlinkDeployment(), ctx.getJosdkContext());
        var triggerIdOpt =
                triggerCheckpointOrSavepoint(resource.getSpec(), ctxFlinkDeployment, jobId);

        if (triggerIdOpt.isEmpty()) {
            LOG.warn("Failed to trigger snapshot {}", resource.getMetadata().getName());
            return;
        }

        resource.getMetadata()
                .getLabels()
                .putIfAbsent(CrdConstants.LABEL_SNAPSHOT_TYPE, SnapshotTriggerType.MANUAL.name());
        resource.getStatus().setState(FlinkStateSnapshotState.IN_PROGRESS);
        resource.getStatus().setTriggerId(triggerIdOpt.get());
        resource.getStatus().setTriggerTimestamp(ZonedDateTime.now().format(DATE_TIME_FORMATTER));
    }

    private static Optional<String> triggerCheckpointOrSavepoint(
            FlinkStateSnapshotSpec spec,
            FlinkResourceContext<FlinkDeployment> flinkDeploymentContext,
            String jobId) {
        var flinkService = flinkDeploymentContext.getFlinkService();
        var conf =
                Preconditions.checkNotNull(
                        flinkDeploymentContext.getObserveConfig(),
                        String.format(
                                "Observe config was null for %s",
                                flinkDeploymentContext.getResource().getMetadata().getName()));

        try {
            if (spec.isSavepoint()) {
                var path =
                        ObjectUtils.firstNonNull(
                                spec.getSavepoint().getPath(),
                                conf.get(CheckpointingOptions.SAVEPOINT_DIRECTORY));
                Preconditions.checkNotNull(path, "Savepoint path cannot be determined!");
                return Optional.of(
                        flinkService.triggerSavepoint(
                                jobId,
                                org.apache.flink.core.execution.SavepointFormatType.valueOf(
                                        spec.getSavepoint().getFormatType().name()),
                                path,
                                conf));
            } else {
                return Optional.of(
                        flinkService.triggerCheckpoint(
                                jobId,
                                org.apache.flink.core.execution.CheckpointType.valueOf(
                                        spec.getCheckpoint().getCheckpointType().name()),
                                conf));
            }
        } catch (Exception e) {
            LOG.error(
                    "Failed to trigger snapshot for {} and job ID {}",
                    flinkDeploymentContext.getResource().getMetadata().getName(),
                    jobId);
            return Optional.empty();
        }
    }
}
