package ru.vk.itmo.test.solution;

import java.lang.foreign.MemorySegment;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.solution.MemorySegmentDao;
import ru.vk.itmo.test.DaoFactory;

@DaoFactory(stage = 1)
public class MemorySegmentDaoFactory implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {

    @Override
    public Dao<MemorySegment, Entry<MemorySegment>> createDao(Config config) {
        return new MemorySegmentDao(config);
    }

    @Override
    public String toString(MemorySegment memorySegment) {
        // TODO implement
        return null;
    }

    @Override
    public MemorySegment fromString(String data) {
        // TODO implement
        return null;
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }
}
