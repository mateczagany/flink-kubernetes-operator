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

package org.apache.flink.kubernetes.operator.observer.snapshot;

import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotState;
import org.apache.flink.kubernetes.operator.controller.FlinkStateSnapshotContext;
import org.apache.flink.kubernetes.operator.service.FlinkResourceContextFactory;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;

import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/** The observer of {@link org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot}. */
@RequiredArgsConstructor
public class StateSnapshotObserver {

    private static final Logger LOG = LoggerFactory.getLogger(StateSnapshotObserver.class);
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX");

    private final FlinkResourceContextFactory ctxFactory;
    private final EventRecorder eventRecorder;

    public void observe(FlinkStateSnapshotContext ctx) {
        var resource = ctx.getResource();
        var savepointState = resource.getStatus().getState();

        if (FlinkStateSnapshotState.IN_PROGRESS.equals(savepointState)) {
            observeSavepointState(ctx);
        }
    }

    private void observeSavepointState(FlinkStateSnapshotContext ctx) {
        var resource = ctx.getResource();
        var resourceName = resource.getMetadata().getName();
        var triggerId = resource.getStatus().getTriggerId();

        LOG.info("Observing savepoint state for resource {}...", resourceName);

        if (StringUtils.isEmpty(triggerId)) {
            LOG.debug("Trigger ID is not set for savepoint {} yet.", resourceName);
            return;
        }

        var secondaryResource =
                ctx.getSecondaryResource()
                        .orElseThrow(
                                () ->
                                        new RuntimeException(
                                                String.format(
                                                        "Secondary resource %s for savepoint %s was not found",
                                                        resource.getSpec().getJobReference(),
                                                        resourceName)));
        var jobId = secondaryResource.getStatus().getJobStatus().getJobId();

        var ctxFlinkDeployment =
                ctxFactory.getResourceContext(
                        ctx.getReferencedJobFlinkDeployment(), ctx.getJosdkContext());
        var observeConfig = ctx.getReferencedJobObserveConfig();

        // TODO: check if job is not running, set to abandoned
        if (resource.getSpec().isSavepoint()) {
            var savepointInfo =
                    ctxFlinkDeployment
                            .getFlinkService()
                            .fetchSavepointInfo(triggerId, jobId, observeConfig);
            if (savepointInfo.isPending()) {
                LOG.debug("Savepoint for {} with ID {} is pending", resourceName, triggerId);
                return;
            }

            if (savepointInfo.getError() != null) {
                LOG.error("Savepoint {} failed: {}", resourceName, savepointInfo.getError());
                snapshotFailed(ctx.getKubernetesClient(), resource, savepointInfo.getError());
            } else {
                LOG.info("Savepoint {} successful: {}", resourceName, savepointInfo.getLocation());
                snapshotSuccessful(resource, savepointInfo.getLocation());
            }
        } else {
            var checkpointInfo =
                    ctxFlinkDeployment
                            .getFlinkService()
                            .fetchCheckpointInfo(triggerId, jobId, observeConfig);
            if (checkpointInfo.isPending()) {
                LOG.debug("Checkpoint for {} with ID {} is pending", resourceName, triggerId);
                return;
            }

            if (checkpointInfo.getError() != null) {
                LOG.error("Checkpoint {} failed: {}", resourceName, checkpointInfo.getError());
                snapshotFailed(ctx.getKubernetesClient(), resource, checkpointInfo.getError());
            } else {
                LOG.info(
                        "Checkpoint {} was successful, querying final checkpoint path...",
                        resourceName);
                var checkpointStats =
                        ctxFlinkDeployment
                                .getFlinkService()
                                .fetchCheckpointStatistics(
                                        jobId, checkpointInfo.getCheckpointId(), observeConfig);
                if (checkpointStats.isPresent()) {
                    var checkpointPath = checkpointStats.get().getExternalPath();
                    LOG.info("Checkpoint {} successful: {}", resourceName, checkpointPath);
                    snapshotSuccessful(resource, checkpointPath);
                } else {
                    LOG.error(
                            "Checkpoint {} was successful, querying final checkpoint path...",
                            resourceName);
                }
            }
        }
    }

    private void snapshotFailed(
            KubernetesClient kubernetesClient, FlinkStateSnapshot snapshot, String error) {
        var reason =
                snapshot.getSpec().isSavepoint()
                        ? EventRecorder.Reason.SavepointError
                        : EventRecorder.Reason.CheckpointError;
        eventRecorder.triggerSnapshotEvent(
                snapshot,
                EventRecorder.Type.Warning,
                reason,
                EventRecorder.Component.Snapshot,
                String.format("Snapshot failed with error '%s'", error),
                kubernetesClient);

        snapshot.getStatus().setState(FlinkStateSnapshotState.FAILED);
        snapshot.getStatus().setError(error);
        snapshot.getStatus().setFailures(snapshot.getStatus().getFailures() + 1);
        snapshot.getStatus().setResultTimestamp(ZonedDateTime.now().format(DATE_TIME_FORMATTER));
    }

    private void snapshotSuccessful(FlinkStateSnapshot snapshot, String location) {
        snapshot.getStatus().setState(FlinkStateSnapshotState.COMPLETED);
        snapshot.getStatus().setPath(location);
        snapshot.getStatus().setError(null);
        snapshot.getStatus().setResultTimestamp(ZonedDateTime.now().format(DATE_TIME_FORMATTER));
    }
}
