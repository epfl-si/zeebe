/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.gateway.impl.broker.request;

import io.camunda.zeebe.protocol.impl.record.value.job.JobRecord;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.JobIntent;
import org.agrona.DirectBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BrokerCompleteJobRequest extends BrokerExecuteCommand<JobRecord> {

  private static final Logger LOGGER = LoggerFactory.getLogger(BrokerCompleteJobRequest.class);
  private final JobRecord requestDto = new JobRecord();

  public BrokerCompleteJobRequest(final long key, final DirectBuffer variables) {
    super(ValueType.JOB, JobIntent.COMPLETE);
    LOGGER.info("BrokerCompleteJobRequest with variables of size {}", variables.capacity());
    request.setKey(key);
    requestDto.setVariables(variables);
  }

  @Override
  public JobRecord getRequestWriter() {
    return requestDto;
  }

  @Override
  protected JobRecord toResponseDto(final DirectBuffer buffer) {
    final JobRecord responseDto = new JobRecord();
    responseDto.wrap(buffer);
    return responseDto;
  }
}
