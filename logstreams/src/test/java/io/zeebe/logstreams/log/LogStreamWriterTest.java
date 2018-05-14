/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.logstreams.log;

import static io.zeebe.util.buffer.BufferUtil.wrapString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import io.zeebe.logstreams.util.*;
import io.zeebe.util.buffer.DirectBufferWriter;
import io.zeebe.util.sched.future.ActorFuture;
import io.zeebe.util.sched.testing.ControlledActorSchedulerRule;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.*;
import org.junit.rules.*;

public class LogStreamWriterTest
{
    private static final DirectBuffer EVENT_VALUE = wrapString("value");
    private static final DirectBuffer EVENT_METADATA = wrapString("metadata");

    /**
     * used by some test to write to the logstream in an actor thread.
     */
    @Rule
    public final ControlledActorSchedulerRule writerScheduler = new ControlledActorSchedulerRule();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    public LogStreamRule logStreamRule = new LogStreamRule(temporaryFolder);
    public LogStreamReaderRule readerRule = new LogStreamReaderRule(logStreamRule);
    public LogStreamWriterRule writerRule = new LogStreamWriterRule(logStreamRule);

    @Rule
    public RuleChain ruleChain =
        RuleChain.outerRule(temporaryFolder)
                 .around(logStreamRule)
                 .around(writerRule)
                 .around(readerRule);

    private LogStreamWriter writer;

    @Before
    public void setUp()
    {
        writer = new LogStreamWriterImpl(logStreamRule.getLogStream());

        logStreamRule.setCommitPosition(Long.MAX_VALUE);
    }

    private LoggedEvent getWrittenEvent(long position)
    {
        assertThat(position).isGreaterThan(0);

        writerRule.waitForPositionToBeAppended(position);

        final LoggedEvent event = readerRule.readEventAtPosition(position);

        assertThat(event)
            .withFailMessage("No written event found at position: {}", position)
            .isNotNull();

        return event;
    }

    @Test
    public void shouldReturnPositionOfWrittenEvent()
    {
        // when
        final long position = writer
            .positionAsKey()
            .value(EVENT_VALUE)
            .tryWrite();

        // then
        assertThat(position).isGreaterThan(0);

        final LoggedEvent event = getWrittenEvent(position);
        assertThat(event.getPosition()).isEqualTo(position);
    }

    @Test
    public void shouldWriteEventWithValueBuffer()
    {
        // when
        final long position = writer
            .positionAsKey()
            .value(EVENT_VALUE)
            .tryWrite();

        // then
        final LoggedEvent event = getWrittenEvent(position);
        final DirectBuffer valueBuffer = event.getValueBuffer();
        final UnsafeBuffer value = new UnsafeBuffer(valueBuffer, event.getValueOffset(), event.getValueLength());

        assertThat(value).isEqualTo(EVENT_VALUE);
    }

    @Test
    public void shouldWriteEventWithValueBufferPartially()
    {
        // when
        final long position = writer
            .positionAsKey()
            .value(EVENT_VALUE, 1, 2)
            .tryWrite();

        // then
        final LoggedEvent event = getWrittenEvent(position);
        final DirectBuffer valueBuffer = event.getValueBuffer();
        final UnsafeBuffer value = new UnsafeBuffer(valueBuffer, event.getValueOffset(), event.getValueLength());

        assertThat(value).isEqualTo(new UnsafeBuffer(EVENT_VALUE, 1, 2));
    }

    @Test
    public void shouldWriteEventWithValueWriter()
    {
        // when
        final long position = writer
            .positionAsKey()
            .valueWriter(new DirectBufferWriter().wrap(EVENT_VALUE))
            .tryWrite();

        // then
        final LoggedEvent event = getWrittenEvent(position);
        final DirectBuffer valueBuffer = event.getValueBuffer();
        final UnsafeBuffer value = new UnsafeBuffer(valueBuffer, event.getValueOffset(), event.getValueLength());

        assertThat(value).isEqualTo(EVENT_VALUE);
    }

    @Test
    public void shouldWriteEventWithMetadataBuffer()
    {
        // when
        final long position = writer
            .positionAsKey()
            .value(EVENT_VALUE)
            .metadata(EVENT_METADATA)
            .tryWrite();

        // then
        final LoggedEvent event = getWrittenEvent(position);
        final DirectBuffer metadataBuffer = event.getMetadata();
        final UnsafeBuffer metadata = new UnsafeBuffer(metadataBuffer, event.getMetadataOffset(), event.getMetadataLength());

        assertThat(metadata).isEqualTo(EVENT_METADATA);
    }

    @Test
    public void shouldWriteEventWithMetadataBufferPartially()
    {
        // when
        final long position = writer
            .positionAsKey()
            .value(EVENT_VALUE)
            .metadata(EVENT_METADATA, 1, 2)
            .tryWrite();

        // then
        final LoggedEvent event = getWrittenEvent(position);
        final DirectBuffer metadataBuffer = event.getMetadata();
        final UnsafeBuffer metadata = new UnsafeBuffer(metadataBuffer, event.getMetadataOffset(), event.getMetadataLength());

        assertThat(metadata).isEqualTo(new UnsafeBuffer(EVENT_METADATA, 1, 2));
    }

    @Test
    public void shouldWriteEventWithMetadataWriter()
    {
        // when
        final long position = writer
            .positionAsKey()
            .value(EVENT_VALUE)
            .metadataWriter(new DirectBufferWriter().wrap(EVENT_METADATA))
            .tryWrite();

        // then
        final LoggedEvent event = getWrittenEvent(position);
        final DirectBuffer metadataBuffer = event.getMetadata();
        final UnsafeBuffer metadata = new UnsafeBuffer(metadataBuffer, event.getMetadataOffset(), event.getMetadataLength());

        assertThat(metadata).isEqualTo(EVENT_METADATA);
    }

    @Test
    public void shouldWriteEventWithKey()
    {
        // when
        final long position = writer
            .key(123L)
            .value(EVENT_VALUE)
            .tryWrite();

        // then
        assertThat(getWrittenEvent(position).getKey()).isEqualTo(123L);
    }

    @Test
    public void shouldWriteEventWithTimestamp() throws InterruptedException, ExecutionException
    {
        final Callable<Long> doWrite = () -> writer.positionAsKey().value(EVENT_VALUE).tryWrite();

        // given
        final long firstTimestamp = System.currentTimeMillis();
        writerScheduler.getClock().setCurrentTime(firstTimestamp);

        // when
        final ActorFuture<Long> firstPosition = writerScheduler.call(doWrite);
        writerScheduler.workUntilDone();

        // then
        assertThat(getWrittenEvent(firstPosition.get()).getTimestamp())
            .isEqualTo(firstTimestamp);

        // given
        final long secondTimestamp = firstTimestamp + 1_000;
        writerScheduler.getClock().setCurrentTime(secondTimestamp);

        // when
        final ActorFuture<Long> secondPosition = writerScheduler.call(doWrite);
        writerScheduler.workUntilDone();

        // then
        assertThat(getWrittenEvent(secondPosition.get()).getTimestamp())
            .isEqualTo(secondTimestamp);
    }

    @Test
    public void shouldWriteEventWithPositionAsKey()
    {
        // when
        final long position = writer
            .positionAsKey()
            .value(EVENT_VALUE)
            .tryWrite();

        // then
        assertThat(getWrittenEvent(position).getKey()).isEqualTo(position);
    }

    @Test
    public void shouldWriteEventWithSourceEvent()
    {
        // when
        final long position = writer
            .positionAsKey()
            .value(EVENT_VALUE)
            .sourceEvent(4, 123L)
            .tryWrite();

        // then
        final LoggedEvent event = getWrittenEvent(position);
        assertThat(event.getSourceEventLogStreamPartitionId()).isEqualTo(4);
        assertThat(event.getSourceEventPosition()).isEqualTo(123L);
    }

    @Test
    public void shouldWriteEventWithoutSourceEvent()
    {
        // when
        final long position = writer
            .positionAsKey()
            .value(EVENT_VALUE)
            .tryWrite();

        // then
        final LoggedEvent event = getWrittenEvent(position);
        assertThat(event.getSourceEventLogStreamPartitionId()).isEqualTo(-1);
        assertThat(event.getSourceEventPosition()).isEqualTo(-1L);
    }

    @Test
    public void shouldWriteEventWithProducerId()
    {
        // when
        final long position = writer
            .positionAsKey()
            .value(EVENT_VALUE)
            .producerId(123)
            .tryWrite();

        // then
        assertThat(getWrittenEvent(position).getProducerId()).isEqualTo(123);
    }

    @Test
    public void shouldWriteEventWithoutProducerId()
    {
        // when
        final long position = writer
            .positionAsKey()
            .value(EVENT_VALUE)
            .tryWrite();

        // then
        assertThat(getWrittenEvent(position).getProducerId()).isEqualTo(-1);
    }

    @Test
    public void shouldWriteEventWithRaftTerm()
    {
        // given
        logStreamRule.getLogStream().setTerm(123);

        // when
        final long position = writer
            .positionAsKey()
            .value(EVENT_VALUE)
            .tryWrite();

        // then
        assertThat(getWrittenEvent(position).getRaftTerm()).isEqualTo(123);
    }

    @Test
    public void shouldFailToWriteEventWithoutKey()
    {
        // when
        assertThatThrownBy(() ->
        {
            writer
                .value(EVENT_VALUE)
                .tryWrite();
        })
            .isInstanceOf(RuntimeException.class)
            .hasMessage("key must be greater than or equal to 0");
    }

    @Test
    public void shouldFailToWriteEventWithoutValue()
    {
        // when
        assertThatThrownBy(() ->
        {
            writer
                .positionAsKey()
                .tryWrite();
        })
            .isInstanceOf(RuntimeException.class)
            .hasMessage("value must not be null");
    }

}
