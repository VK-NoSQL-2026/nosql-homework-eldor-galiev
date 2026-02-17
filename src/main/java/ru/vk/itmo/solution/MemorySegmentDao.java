package ru.vk.itmo.solution;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        // TODO implement
        return null;
    }

    public MemorySegmentDao(Config config) {
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        // TODO implement
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        // TODO implement
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void close() {
    }
}
