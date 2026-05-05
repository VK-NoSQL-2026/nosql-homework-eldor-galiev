package ru.vk.itmo.test.solution;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.solution.MemorySegmentDao;
import ru.vk.itmo.test.DaoFactory;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;

@DaoFactory(stage = 6)
public class MemorySegmentDaoFactory implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao(Config config) {
        return new MemorySegmentDao(config);
    }

    @Override
    public String toString(MemorySegment memorySegment) {
        if (memorySegment == null) return null;

        byte[] bytes = new byte[(int) memorySegment.byteSize()];
        MemorySegment.copy(memorySegment, ValueLayout.JAVA_BYTE, 0, bytes, 0, bytes.length);

        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public MemorySegment fromString(String data) {
        if (data == null) return null;

        byte[] bytes = data.getBytes(StandardCharsets.UTF_8);

        return MemorySegment.ofArray(bytes);
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}