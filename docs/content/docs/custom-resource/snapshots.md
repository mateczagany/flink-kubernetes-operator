---
title: "Snapshots"
weight: 4
type: docs
aliases:
  - /custom-resource/snapshots.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Snapshots

To create, list, delete snapshots you can use the custom resource called `FlinkStateSnapshot`. The operator will use the same controller flow as in the case of `FlinkDeployment` and `FlinkSessionJob` to trigger the savepoint/checkpoint and observe its status.

This feature deprecates the old `savepointInfo` and `checkpointInfo` fields found in the Flink resource CR status, alongside with spec fields `initialSavepointPath`, `savepointTriggerNonce` and `checkpointTriggerNonce`. It is enabled by default by using `kubernetes.operator.snapshot.resource.enabled`. If you turn this config off, the Operator will keep using the deprecated fields and methods to create and manage snapshots.

## Overview

You can create savepoints and checkpoints using this CR, this will be determined by checking which of the spec fields (`savepoint` and `checkpoint`) is present.

You can instruct the Operator to start a new FlinkDeployment/FlinkSessionJob from an existing savepoint by using `flinkStateSnapshotReference`.

## Manual snapshots

When you create a new `FlinkStateSnapshot` CR, in the first reconciliation phase the operator will trigger the savepoint/checkpoint in the linked deployment via REST API. The resulting trigger ID will be added to the CR Status.

In the next observation phase the operator will check all the in-progress snapshots and query the state of them via REST API. If the snapshot was successful, the path will be added to the CR Status.

## Upgrade savepoints

By default, the operator will create a new `FlinkStateSnapshot` each time an upgrade is triggered or the job is stopped. During restart the job will find the latest created `FlinkStateSnapshot` in the Kubernetes cluster that references the job.

TODO automatic cleanup

## Error handling

If there were any errors during the snapshot trigger/observation, the `error` field will be populated in the CR status and the `failures` field will be incremented by 1. If the backoff limit specified in the spec is reached, the snapshot will enter a `FAILED` state, and won't be retried. If it's not reached, the Operator will continuously back off retrying the snapshot (10s, 20s, 40s, ...).

In case of errors there will also be a new Event generated for the snapshot resource containing the error message.

## Automatic disposal

In case of savepoints you can also ask the operator to automatically dispose the savepoint on the filesystem if the CR gets deleted. This however requires the referenced Flink resource to be alive, because this is done via REST API.
This feature is not available for checkpoints because they are owned by Flink.


TODO: JobReference might not be needed with `alreadyExists`
TODO: Examples

