/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.gateway.impl.broker.request;

import io.camunda.zeebe.protocol.Protocol;
import io.camunda.zeebe.protocol.impl.record.value.deployment.DeploymentRecord;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.DeploymentIntent;
import org.agrona.DirectBuffer;

public final class BrokerDeployRequest extends BrokerExecuteCommand<DeploymentRecord> {

  private final DeploymentRecord requestDto = new DeploymentRecord();

  public BrokerDeployRequest() {
    super(ValueType.DEPLOYMENT, DeploymentIntent.CREATE);
    setPartitionId(Protocol.DEPLOYMENT_PARTITION);
  }

  public BrokerDeployRequest addResource(final byte[] resource, final String resourceName) {
    requestDto.resources().add().setResource(resource).setResourceName(resourceName);

    return this;
  }

  @Override
  public DeploymentRecord getRequestWriter() {
    return requestDto;
  }

  @Override
  protected DeploymentRecord toResponseDto(final DirectBuffer buffer) {
    final DeploymentRecord responseDto = new DeploymentRecord();
    responseDto.wrap(buffer);
    return responseDto;
  }
}
