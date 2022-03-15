#!/bin/sh -eux

mvn dependency:get -B \
    -DremoteRepositories="camunda-nexus::::https://app.camunda.com/nexus/content/repositories/public" \
    -DgroupId="io.camunda" -DartifactId="camunda-zeebe" \
    -Dversion="${VERSION}" -Dpackaging="tar.gz" -Dtransitive=false

mvn dependency:copy -B \
    -Dartifact="io.camunda:camunda-zeebe:${VERSION}:tar.gz" \
    -DoutputDirectory=${WORKSPACE} \
    -Dmdep.stripVersion=true
