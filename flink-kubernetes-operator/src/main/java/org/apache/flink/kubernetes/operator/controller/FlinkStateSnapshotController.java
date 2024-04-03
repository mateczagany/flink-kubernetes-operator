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

package org.apache.flink.kubernetes.operator.controller;

import org.apache.flink.kubernetes.operator.api.FlinkStateSnapshot;
import org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotState;
import org.apache.flink.kubernetes.operator.api.status.FlinkStateSnapshotStatus;
import org.apache.flink.kubernetes.operator.exception.ReconciliationException;
import org.apache.flink.kubernetes.operator.observer.snapshot.StateSnapshotObserver;
import org.apache.flink.kubernetes.operator.reconciler.ReconciliationUtils;
import org.apache.flink.kubernetes.operator.reconciler.snapshot.StateSnapshotReconciler;
import org.apache.flink.kubernetes.operator.service.FlinkResourceContextFactory;
import org.apache.flink.kubernetes.operator.utils.EventRecorder;
import org.apache.flink.kubernetes.operator.utils.EventSourceUtils;
import org.apache.flink.kubernetes.operator.utils.StatusRecorder;
import org.apache.flink.kubernetes.operator.validation.FlinkResourceValidator;

import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusHandler;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Set;

/** Controller that runs the main reconcile loop for {@link FlinkStateSnapshot}. */
@ControllerConfiguration
public class FlinkStateSnapshotController
        implements Reconciler<FlinkStateSnapshot>,
                ErrorStatusHandler<FlinkStateSnapshot>,
                EventSourceInitializer<FlinkStateSnapshot>,
                Cleaner<FlinkStateSnapshot> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkStateSnapshotController.class);

    private final Set<FlinkResourceValidator> validators;
    private final FlinkResourceContextFactory ctxFactory;
    private final StateSnapshotReconciler reconciler;
    private final StateSnapshotObserver observer;
    private final EventRecorder eventRecorder;
    private final StatusRecorder<FlinkStateSnapshot, FlinkStateSnapshotStatus> statusRecorder;

    public FlinkStateSnapshotController(
            Set<FlinkResourceValidator> validators,
            FlinkResourceContextFactory ctxFactory,
            StateSnapshotReconciler reconciler,
            StateSnapshotObserver observer,
            EventRecorder eventRecorder,
            StatusRecorder<FlinkStateSnapshot, FlinkStateSnapshotStatus> statusRecorder) {
        this.validators = validators;
        this.ctxFactory = ctxFactory;
        this.reconciler = reconciler;
        this.observer = observer;
        this.eventRecorder = eventRecorder;
        this.statusRecorder = statusRecorder;
    }

    @Override
    public UpdateControl<FlinkStateSnapshot> reconcile(
            FlinkStateSnapshot flinkStateSnapshot, Context<FlinkStateSnapshot> josdkContext) {
        statusRecorder.updateStatusFromCache(flinkStateSnapshot);

        var ctx = ctxFactory.getSavepointContext(flinkStateSnapshot, josdkContext);

        // observe
        observer.observe(ctx);

        // validate
        if (!validateSavepoint(ctx)) {
            statusRecorder.patchAndCacheStatus(flinkStateSnapshot, ctx.getKubernetesClient());
            UpdateControl<FlinkStateSnapshot> updateControl = UpdateControl.noUpdate();
            return updateControl.rescheduleAfter(
                    ctx.getOperatorConfig().getReconcileInterval().toMillis());
        }

        // reconcile
        try {
            statusRecorder.patchAndCacheStatus(flinkStateSnapshot, ctx.getKubernetesClient());
            reconciler.reconcile(ctx);
        } catch (Exception e) {
            eventRecorder.triggerSnapshotEvent(
                    flinkStateSnapshot,
                    EventRecorder.Type.Warning,
                    "SavepointException",
                    e.getMessage(),
                    EventRecorder.Component.Snapshot,
                    josdkContext.getClient());
            throw new ReconciliationException(e);
        }

        statusRecorder.patchAndCacheStatus(flinkStateSnapshot, ctx.getKubernetesClient());
        return getUpdateControl(ctx);
    }

    @Override
    public DeleteControl cleanup(
            FlinkStateSnapshot flinkStateSnapshot, Context<FlinkStateSnapshot> josdkContext) {
        var resourceName = flinkStateSnapshot.getMetadata().getName();
        LOG.info("Cleaning up resource {}...", resourceName);

        if (flinkStateSnapshot.getSpec().isCheckpoint()) {
            return DeleteControl.defaultDelete();
        }
        if (!flinkStateSnapshot.getSpec().getSavepoint().isDisposeOnDelete()) {
            return DeleteControl.defaultDelete();
        }

        var ctx = ctxFactory.getSavepointContext(flinkStateSnapshot, josdkContext);
        var state = flinkStateSnapshot.getStatus().getState();

        switch (state) {
            case IN_PROGRESS:
                LOG.info(
                        "Cannot delete resource {} yet as savepoint is still in progress...",
                        resourceName);
                return DeleteControl.noFinalizerRemoval()
                        .rescheduleAfter(ctx.getOperatorConfig().getReconcileInterval().toMillis());
            case FAILED:
                LOG.info(
                        "Savepoint was not successful, cleaning up resource {} without disposal...",
                        resourceName);
                return DeleteControl.defaultDelete();
            case TRIGGER_PENDING:
                LOG.info(
                        "Savepoint has not started yet, cleaning up resource {} without disposal...",
                        resourceName);
                return DeleteControl.defaultDelete();
            case COMPLETED:
                return handleSnapshotCleanup(flinkStateSnapshot, ctx);
            default:
                LOG.info("Unknown savepoint state for {}: {}", resourceName, state);
                return DeleteControl.defaultDelete();
        }
    }

    private UpdateControl<FlinkStateSnapshot> getUpdateControl(FlinkStateSnapshotContext ctx) {
        var resource = ctx.getResource();
        if (FlinkStateSnapshotState.FAILED.equals(resource.getStatus().getState())) {
            if (resource.getStatus().getFailures() > resource.getSpec().getBackoffLimit()) {
                LOG.info(
                        "Snapshot {} failed and won't be retried as failure count exceeded the backoff limit",
                        resource.getMetadata().getName());
            } else {
                long retrySeconds = 10L * (1L << resource.getStatus().getFailures() - 1);
                LOG.info(
                        "Snapshot {} failed and will be retried in {} seconds...",
                        resource.getMetadata().getName(),
                        retrySeconds);
                resource.getStatus().setState(FlinkStateSnapshotState.TRIGGER_PENDING);
                return UpdateControl.<FlinkStateSnapshot>noUpdate()
                        .rescheduleAfter(Duration.ofSeconds(retrySeconds));
            }
        }
        return UpdateControl.<FlinkStateSnapshot>noUpdate()
                .rescheduleAfter(ctx.getOperatorConfig().getReconcileInterval().toMillis());
    }

    private DeleteControl handleSnapshotCleanup(
            FlinkStateSnapshot flinkStateSnapshot, FlinkStateSnapshotContext ctx) {
        var resourceName = flinkStateSnapshot.getMetadata().getName();
        String path = flinkStateSnapshot.getStatus().getPath();

        if (path == null) {
            LOG.info("No path was saved for snapshot {}, no cleanup required.", resourceName);
            return DeleteControl.defaultDelete();
        }

        var secondaryResourceOpt = ctx.getSecondaryResource();
        if (secondaryResourceOpt.isEmpty()) {
            LOG.info(
                    "Flink job associated with {} is no longer running, cannot delete resource.",
                    resourceName);
            return DeleteControl.noFinalizerRemoval()
                    .rescheduleAfter(ctx.getOperatorConfig().getReconcileInterval().toMillis());
        }

        LOG.info("Disposing of savepoint of {} before deleting the resource...", resourceName);
        var observeConfig = ctx.getReferencedJobObserveConfig();
        var ctxFlinkDeployment =
                ctxFactory.getResourceContext(
                        ctx.getReferencedJobFlinkDeployment(), ctx.getJosdkContext());
        var flinkService = ctxFlinkDeployment.getFlinkService();

        try {
            flinkService.disposeSavepoint(path, observeConfig);
            return DeleteControl.defaultDelete();
        } catch (Exception e) {
            LOG.error(
                    "Failed to dispose savepoint {} from deployment {}",
                    path,
                    ctxFlinkDeployment.getResource().getMetadata().getName());
            return DeleteControl.noFinalizerRemoval()
                    .rescheduleAfter(ctx.getOperatorConfig().getReconcileInterval().toMillis());
        }
    }

    @Override
    public ErrorStatusUpdateControl<FlinkStateSnapshot> updateErrorStatus(
            FlinkStateSnapshot flinkStateSnapshot,
            Context<FlinkStateSnapshot> context,
            Exception e) {
        var ctx = ctxFactory.getSavepointContext(flinkStateSnapshot, context);
        return ReconciliationUtils.toErrorStatusUpdateControl(ctx, e, statusRecorder);
    }

    @Override
    public Map<String, EventSource> prepareEventSources(
            EventSourceContext<FlinkStateSnapshot> context) {
        return EventSourceInitializer.nameEventSources(
                EventSourceUtils.getFlinkStateSnapshotInformerEventSources(context));
    }

    private boolean validateSavepoint(FlinkStateSnapshotContext ctx) {
        var savepoint = ctx.getResource();
        for (var validator : validators) {
            var validationError =
                    validator.validateStateSnapshot(savepoint, ctx.getSecondaryResource());
            if (validationError.isPresent()) {
                eventRecorder.triggerSnapshotEvent(
                        savepoint,
                        EventRecorder.Type.Warning,
                        EventRecorder.Reason.ValidationError,
                        EventRecorder.Component.Operator,
                        validationError.get(),
                        ctx.getKubernetesClient());
                return true;
            }
        }
        return true;
    }
}
