/*******************************************************************************
 * Copyright (c) quickfixengine.org  All rights reserved.
 *
 * This file is part of the QuickFIX FIX Engine
 *
 * This file may be distributed under the terms of the quickfixengine.org
 * license as defined by quickfixengine.org and appearing in the file
 * LICENSE included in the packaging of this file.
 *
 * This file is provided AS IS with NO WARRANTY OF ANY KIND, INCLUDING
 * THE WARRANTY OF DESIGN, MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE.
 *
 * See http://www.quickfixengine.org/LICENSE for licensing information.
 *
 * Contact ask@quickfixengine.org if any conditions of this licensing
 * are not clear to you.
 ******************************************************************************/

package quickfix;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

class JdbcWriteBackStore implements MessageStore {

    private final MessageStore jdbcStore;
    private final WriteBackStore writeBackStore;

    public JdbcWriteBackStore(MessageStore jdbcStore, WriteBackStore writeBackStore) {
        this.jdbcStore = jdbcStore;
        this.writeBackStore = this.initializeWriteBackStore(writeBackStore);
    }

    private WriteBackStore initializeWriteBackStore(WriteBackStore writeBackStore) {
        if (writeBackStore == null) {
            writeBackStore = new MemoryWriteBackStore();
        }

        writeBackStore.enableScheduling();
        return writeBackStore;
    }

    @Override
    public boolean set(int sequence, String message) throws IOException {
        return writeBackStore.preserve(sequence, message);
    }

    /**
     * Default JdbcStore Wrapper
     */
    @Override
    public void get(int startSequence, int endSequence, Collection<String> messages) throws IOException {
        jdbcStore.get(startSequence, endSequence, messages);
    }

    @Override
    public int getNextSenderMsgSeqNum() throws IOException {
        return jdbcStore.getNextSenderMsgSeqNum();
    }

    @Override
    public int getNextTargetMsgSeqNum() throws IOException {
        return jdbcStore.getNextTargetMsgSeqNum();
    }

    @Override
    public void setNextSenderMsgSeqNum(int next) throws IOException {
        jdbcStore.setNextSenderMsgSeqNum(next);
    }

    @Override
    public void setNextTargetMsgSeqNum(int next) throws IOException {
        jdbcStore.setNextTargetMsgSeqNum(next);
    }

    @Override
    public void incrNextSenderMsgSeqNum() throws IOException {
        jdbcStore.incrNextSenderMsgSeqNum();
    }

    @Override
    public void incrNextTargetMsgSeqNum() throws IOException {
        jdbcStore.incrNextTargetMsgSeqNum();
    }

    @Override
    public Date getCreationTime() throws IOException {
        return jdbcStore.getCreationTime();
    }

    @Override
    public void reset() throws IOException {
        jdbcStore.reset();
    }

    @Override
    public void refresh() throws IOException {
        jdbcStore.refresh();
    }

    public interface WriteBackStore {
        boolean preserve(int sequence, String message);

        void enableScheduling();

        void writeBack() throws RuntimeException;
    }

    public class MemoryWriteBackStore implements WriteBackStore {

        private final ConcurrentHashMap<Integer, String> CACHE = new ConcurrentHashMap<>();

        @Override
        public boolean preserve(int sequence, String message) {
            CACHE.put(sequence, message);
            return true;
        }

        @Override
        public void enableScheduling() throws RuntimeException {
            Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(
                    this::writeBack,
                    0,
                    100,
                    TimeUnit.MILLISECONDS
            );
        }

        @Override
        public void writeBack() throws RuntimeException {
            List<Integer> sequences = CACHE.keySet()
                    .stream()
                    .sorted()
                    .collect(Collectors.toList());

            for (final int sequence : sequences) {
                String message = CACHE.get(sequence);
                try {
                    jdbcStore.set(sequence, message);
                    CACHE.remove(sequence);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
