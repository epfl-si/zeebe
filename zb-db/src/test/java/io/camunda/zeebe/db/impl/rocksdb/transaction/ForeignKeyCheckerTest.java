/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.camunda.zeebe.db.impl.rocksdb.transaction;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.camunda.zeebe.db.ConsistencyChecksSettings;
import io.camunda.zeebe.db.ZeebeDbInconsistentException;
import io.camunda.zeebe.db.impl.DbForeignKey;
import io.camunda.zeebe.db.impl.DbLong;
import io.camunda.zeebe.db.impl.DbNil;
import io.camunda.zeebe.db.impl.DefaultZeebeDbFactory;
import java.io.File;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class ForeignKeyCheckerTest {

  @Test
  void shouldFailOnMissingForeignKey() throws Exception {
    // given
    final var db = mock(ZeebeTransactionDb.class);
    final var tx = mock(ZeebeTransaction.class);
    final var check = new ForeignKeyChecker(db, new ConsistencyChecksSettings(true, true));
    final var key = new DbLong();
    key.wrapLong(1);

    // when
    when(tx.get(anyLong(), anyLong(), any(), anyInt())).thenReturn(null);

    // then
    assertThatThrownBy(
            () ->
                check.assertExists(
                    tx, new DbForeignKey<>(key, TestColumnFamilies.TEST_COLUMN_FAMILY)))
        .isInstanceOf(ZeebeDbInconsistentException.class);
  }

  @Test
  void shouldSucceedOnExistingForeignKey() throws Exception {
    // given
    final var db = mock(ZeebeTransactionDb.class);
    final var tx = mock(ZeebeTransaction.class);
    final var check = new ForeignKeyChecker(db, new ConsistencyChecksSettings(true, true));
    final var key = new DbLong();
    key.wrapLong(1);

    // when -- tx says every key exists
    when(tx.get(anyLong(), anyLong(), any(), anyInt())).thenReturn(new byte[] {});

    // then -- check doesn't trow
    check.assertExists(tx, new DbForeignKey<>(key, TestColumnFamilies.TEST_COLUMN_FAMILY));
  }

  @Test
  void shouldSucceedOnRealColumnFamily(@TempDir final File tempDir) throws Exception {
    // given
    final var db = DefaultZeebeDbFactory.<TestColumnFamilies>getDefaultFactory().createDb(tempDir);
    final var txContext = db.createContext();

    final var cf1 =
        db.createColumnFamily(
            TestColumnFamilies.TEST_COLUMN_FAMILY, txContext, new DbLong(), DbNil.INSTANCE);

    final var check =
        new ForeignKeyChecker(
            (ZeebeTransactionDb<?>) db, new ConsistencyChecksSettings(true, true));

    // when -- key 1 exists in first column family
    final var cf1Key = new DbLong();
    cf1Key.wrapLong(1);
    cf1.insert(cf1Key, DbNil.INSTANCE);

    // then -- referring to key 1 does not throw
    assertDoesNotThrow(
        () ->
            check.assertExists(
                (ZeebeTransaction) txContext.getCurrentTransaction(),
                new DbForeignKey<>(cf1Key, TestColumnFamilies.TEST_COLUMN_FAMILY)));

    db.close();
  }

  private enum TestColumnFamilies {
    TEST_COLUMN_FAMILY
  }
}
