package ru.vk.itmo.solution;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage =
            new ConcurrentSkipListMap<>(this::compare);

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> subMap;

        if (from == null && to == null) {
            subMap = storage;
        } else if (from == null) {
            subMap = storage.headMap(to, false);
        } else if (to == null) {
            subMap = storage.tailMap(from, true);
        } else {
            subMap = storage.subMap(from, true, to, false);
        }

        return subMap.values().iterator();
    }

    public MemorySegmentDao(Config config) {
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (key == null) return null;
        return storage.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) return;
        if (entry.value() == null) {
            storage.remove(entry.key());
        } else {
            storage.put(entry.key(), entry);
        }
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public void close() {
    }

    private int compare(MemorySegment segment1, MemorySegment segment2) {
        long offset = segment1.mismatch(segment2);
        if (offset == -1) return 0;
        if (offset == segment1.byteSize()) return -1;
        if (offset == segment2.byteSize()) return 1;

        return Byte.compareUnsigned(
                segment1.get(ValueLayout.JAVA_BYTE, offset),
                segment2.get(ValueLayout.JAVA_BYTE, offset)
        );
    }
}
