/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.engine.processing.job;

import io.camunda.zeebe.engine.metrics.JobMetrics;
import io.camunda.zeebe.engine.processing.common.EventHandle;
import io.camunda.zeebe.engine.processing.streamprocessor.CommandProcessor;
import io.camunda.zeebe.engine.processing.streamprocessor.TypedRecord;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.StateWriter;
import io.camunda.zeebe.engine.processing.streamprocessor.writers.TypedCommandWriter;
import io.camunda.zeebe.engine.state.immutable.ElementInstanceState;
import io.camunda.zeebe.engine.state.immutable.JobState;
import io.camunda.zeebe.engine.state.immutable.ZeebeState;
import io.camunda.zeebe.engine.state.instance.ElementInstance;
import io.camunda.zeebe.protocol.impl.record.value.job.JobRecord;
import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.protocol.record.intent.JobIntent;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class JobCompleteProcessor implements CommandProcessor<JobRecord> {
  private static final Logger LOGGER = LoggerFactory.getLogger(JobCompleteProcessor.class);

  private final JobState jobState;
  private final ElementInstanceState elementInstanceState;
  private final DefaultJobCommandPreconditionGuard defaultProcessor;
  private final JobMetrics jobMetrics;
  private final EventHandle eventHandle;

  public JobCompleteProcessor(
      final ZeebeState state, final JobMetrics jobMetrics, final EventHandle eventHandle) {
    jobState = state.getJobState();
    elementInstanceState = state.getElementInstanceState();
    defaultProcessor =
        new DefaultJobCommandPreconditionGuard(
            "complete",
            jobState,
            (record, commandControl, sideEffect) -> acceptCommand(record, commandControl));
    this.jobMetrics = jobMetrics;
    this.eventHandle = eventHandle;
  }

  @Override
  public boolean onCommand(
      final TypedRecord<JobRecord> command, final CommandControl<JobRecord> commandControl) {

    LOGGER.info("JobCompleteProcessor::onCommand");

    return defaultProcessor.onCommand(command, commandControl);
  }

  @Override
  public void afterAccept(
      final TypedCommandWriter commandWriter,
      final StateWriter stateWriter,
      final long key,
      final Intent intent,
      final JobRecord value) {

    LOGGER.info("JobCompleteProcessor::afterAccept");

    final var serviceTaskKey = value.getElementInstanceKey();

    final ElementInstance serviceTask = elementInstanceState.getInstance(serviceTaskKey);

    if (serviceTask != null) {
      LOGGER.info("JobCompleteProcessor::afterAccept: serviceTask != null");
      final long scopeKey = serviceTask.getValue().getFlowScopeKey();
      final ElementInstance scopeInstance = elementInstanceState.getInstance(scopeKey);

      if (scopeInstance != null && scopeInstance.isActive()) {
        LOGGER.info("JobCompleteProcessor::afterAccept: the scope instance is active");
        eventHandle.triggeringProcessEvent(value);
        commandWriter.appendFollowUpCommand(
            serviceTaskKey, ProcessInstanceIntent.COMPLETE_ELEMENT, serviceTask.getValue());
      }
    }
  }

  private void acceptCommand(
      final TypedRecord<JobRecord> command, final CommandControl<JobRecord> commandControl) {

    final long jobKey = command.getKey();
    LOGGER.info("JobCompleteProcessor::acceptCommand on jobKey {}", jobKey);

    final JobRecord job = jobState.getJob(jobKey);

    job.setVariables(command.getValue().getVariablesBuffer());

    LOGGER.info(
        "JobCompleteProcessor::acceptCommand: setVariables done on {}",
        command.getValue().getVariablesBuffer().toString());

    commandControl.accept(JobIntent.COMPLETED, job);
    jobMetrics.jobCompleted(job.getType());
  }
}
