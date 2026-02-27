/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sushanb;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Filters.Filter;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.TableId;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import org.threeten.bp.Duration;

/**
 * Usage mvn compile exec:java -Dexec.mainClass="com.sushanb.HedgedClient" -Dproject="" -Dinstance="" -Dtable="" -Ddeadline_primary="2000" -Ddeadline_secondary="1000"
 */

public class HedgedClient {
    private static final Logger logger = Logger.getLogger(HedgedClient.class.getName());

    public static BigtableDataClient createClient(
            String projectId, String instanceId, String appProfileId, Duration timeout)
            throws IOException {
        BigtableDataSettings.Builder settingsBuilder =
                BigtableDataSettings.newBuilder()
                        .setProjectId(projectId)
                        .setInstanceId(instanceId)
                        .setAppProfileId(appProfileId);

        RetrySettings retrySettings =
                settingsBuilder.stubSettings().readRowSettings().getRetrySettings().toBuilder()
                        .setInitialRpcTimeout(timeout)
                        .setTotalTimeout(timeout)
                        .setMaxRpcTimeout(timeout)
                        .build();

        settingsBuilder.stubSettings().readRowSettings().setRetrySettings(retrySettings);

        BigtableDataSettings settings = settingsBuilder.build();
        BigtableDataClient client = BigtableDataClient.create(settings);
        logger.info(
                String.format(
                        "BigtableDataClient created successfully for profile: %s with %dms timeout",
                        appProfileId, timeout.toMillis()));
        return client;
    }

    public static CompletableFuture<Row> executeSingleAsyncRead(
            BigtableDataClient client, String tableId, String rowKeyString) {
        TableId targetId = TableId.of(tableId);
        ByteString rowKey = ByteString.copyFromUtf8(rowKeyString);
        Filter filter = Filters.FILTERS.pass();

        ApiFuture<Row> apiFuture = client.readRowAsync(targetId, rowKey, filter);
        CompletableFuture<Row> completableFuture = new CompletableFuture<>();

        ApiFutures.addCallback(
                apiFuture,
                new ApiFutureCallback<Row>() {
                    @Override
                    public void onSuccess(Row result) {
                        completableFuture.complete(result);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        completableFuture.completeExceptionally(t);
                    }
                },
                MoreExecutors.directExecutor());

        return completableFuture;
    }

    public static CompletableFuture<Row> executeHedgedAsyncRead(
            BigtableDataClient primaryClient,
            BigtableDataClient secondaryClient,
            String tableId,
            String rowKey,
            ScheduledExecutorService scheduler,
            long delay,
            TimeUnit unit) {

        CompletableFuture<Row> hedgedRead = new CompletableFuture<>();

        logger.info("Initiating read on PRIMARY client...");
        CompletableFuture<Row> primaryFuture = executeSingleAsyncRead(primaryClient, tableId, rowKey);

        primaryFuture.whenComplete(
                (row, throwable) -> {
                    if (throwable == null) {
                        if (hedgedRead.complete(row)) {
                            logger.info(">>> PRIMARY client won the race and returned the response.");
                        }
                    }
                });

        scheduler.schedule(
                () -> {
                    if (!hedgedRead.isDone()) {
                        logger.warning(
                                String.format(
                                        "%d %s elapsed without primary success. Firing hedged read to SECONDARY client...",
                                        delay, unit.name().toLowerCase()));

                        CompletableFuture<Row> secondaryFuture =
                                executeSingleAsyncRead(secondaryClient, tableId, rowKey);

                        secondaryFuture.whenComplete(
                                (row, throwable) -> {
                                    if (throwable == null) {
                                        if (hedgedRead.complete(row)) {
                                            logger.info(">>> SECONDARY client won the race and returned the response.");
                                        }
                                    } else if (primaryFuture.isCompletedExceptionally()) {
                                        hedgedRead.completeExceptionally(throwable);
                                    }
                                });

                        primaryFuture.whenComplete(
                                (row, throwable) -> {
                                    if (throwable != null && secondaryFuture.isCompletedExceptionally()) {
                                        hedgedRead.completeExceptionally(throwable);
                                    }
                                });
                    }
                },
                delay,
                unit);

        return hedgedRead;
    }

    public static void main(String[] args) {
        String projectId = System.getProperty("project", "");
        String instanceId = System.getProperty("instance", "");
        String tableId = System.getProperty("table", "");
        long deadlinePrimary = Long.parseLong(System.getProperty("deadline_primary", ""));
        long deadlineSecondary = Long.parseLong(System.getProperty("deadline_secondary", ""));

        // Hardcoded for this repro, but you could easily make this a flag too!
        String rowKey = "";

        // hardcode to 1
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        try (BigtableDataClient primaryClient = createClient(projectId, instanceId, "asia-east1", Duration.ofMillis(deadlinePrimary));
             BigtableDataClient secondaryClient = createClient(projectId, instanceId, "us-east4", Duration.ofMillis(deadlineSecondary))) {

            CompletableFuture<Row> hedgedRead =
                    executeHedgedAsyncRead(
                            primaryClient, secondaryClient, tableId, rowKey, scheduler, 7, TimeUnit.MILLISECONDS);

            try {
                Row row = hedgedRead.get();
                if (row != null) {
                    logger.info("Successfully read row! Key: " + row.getKey().toStringUtf8());
                } else {
                    logger.info("Row not found (expected empty response).");
                }
            } catch (Exception e) {
                logger.severe("Both clients failed or timed out: " + e.getMessage());
            }

            logger.info("Read resolved. Shutting down.");

        } catch (Exception e) {
            logger.severe("Application encountered a top-level error: " + e.getMessage());
        } finally {
            scheduler.shutdownNow();
        }
    }
}