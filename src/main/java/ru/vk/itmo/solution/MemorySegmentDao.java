package ru.vk.itmo.solution;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final String FILE_PREFIX = "sstable_";
    static final Comparator<MemorySegment> COMPARATOR = MemorySegmentDao::compare;

    private final Path baseDir;
    private final List<FileStorage> fileStorages;
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> memStorage;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicInteger nextFileId;
    private final Arena arena;

    public MemorySegmentDao(Config config) {
        this.baseDir = config.basePath();
        this.memStorage = new ConcurrentSkipListMap<>(COMPARATOR);
        this.fileStorages = new CopyOnWriteArrayList<>();
        this.arena = Arena.ofShared();

        try {
            if (!Files.exists(baseDir)) {
                Files.createDirectories(baseDir);
                this.nextFileId = new AtomicInteger(0);
                return;
            }

            List<Path> existingFiles = new ArrayList<>();
            try (var files = Files.list(baseDir)) {
                files.filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().startsWith(FILE_PREFIX))
                        .forEach(existingFiles::add);
            }

            int maxId = 0;
            for (Path p : existingFiles) {
                String name = p.getFileName().toString();
                int id = Integer.parseInt(name.substring(FILE_PREFIX.length()));
                maxId = Math.max(maxId, id);
            }
            this.nextFileId = new AtomicInteger(maxId);

            existingFiles.sort((a, b) -> {
                int idA = Integer.parseInt(a.getFileName().toString().substring(FILE_PREFIX.length()));
                int idB = Integer.parseInt(b.getFileName().toString().substring(FILE_PREFIX.length()));
                return Integer.compare(idB, idA);
            });

            for (Path p : existingFiles) {
                fileStorages.add(new FileStorage(p, arena));
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize DAO", e);
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (closed.get()) {
            throw new IllegalStateException("DAO is closed");
        }

        Iterator<Entry<MemorySegment>> memIter = subMapIterator(memStorage, from, to);

        List<Iterator<Entry<MemorySegment>>> fileIters = new ArrayList<>(fileStorages.size());
        for (FileStorage fs : fileStorages) {
            fileIters.add(fs.rangeIterator(from, to));
        }

        return new MergingIterator(memIter, fileIters);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (key == null || closed.get()) {
            return null;
        }

        Entry<MemorySegment> memEntry = memStorage.get(key);
        if (memEntry != null) {
            return memEntry.value() == null ? null : memEntry;
        }

        for (FileStorage fs : fileStorages) {
            Entry<MemorySegment> entry = fs.get(key);
            if (entry != null) {
                return entry.value() == null ? null : entry;
            }
        }

        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null || closed.get()) {
            return;
        }
        memStorage.put(entry.key(), entry);
    }

    @Override
    public void flush() {
        if (closed.get()) {
            return;
        }
        if (memStorage.isEmpty()) {
            return;
        }

        try {
            saveMemTableToDisk();
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush", e);
        }
    }

    @Override
    public void compact() {
        if (closed.get()) {
            throw new IllegalStateException("DAO is closed");
        }

        try {
            List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
            iterators.add(memStorage.values().iterator());
            for (FileStorage fs : fileStorages) {
                iterators.add(fs.rangeIterator(null, null));
            }
            MergingIterator mergingIterator = new MergingIterator(iterators);

            List<Entry<MemorySegment>> aliveEntries = new ArrayList<>();
            while (mergingIterator.hasNext()) {
                aliveEntries.add(mergingIterator.next());
            }

            for (FileStorage fs : fileStorages) {
                Files.deleteIfExists(fs.getPath());
            }
            fileStorages.clear();
            memStorage.clear();

            if (!aliveEntries.isEmpty()) {
                int newFileId = nextFileId.incrementAndGet();
                Path newFile = baseDir.resolve(String.format(FILE_PREFIX + "%05d", newFileId));
                FileStorage newStorage = new FileStorage(newFile, aliveEntries, arena);
                fileStorages.add(newStorage);
            } else {
                nextFileId.set(0);
            }
        } catch (IOException e) {
            throw new RuntimeException("Compact failed", e);
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        try {
            if (!memStorage.isEmpty()) {
                saveMemTableToDisk();
            }
            memStorage.clear();
            fileStorages.clear();
        } catch (IOException e) {
            throw new RuntimeException("Failed to close DAO", e);
        }
    }

    private void saveMemTableToDisk() throws IOException {
        int fileId = nextFileId.incrementAndGet();
        Path newFile = baseDir.resolve(String.format(FILE_PREFIX + "%05d", fileId));

        FileStorage newStorage = new FileStorage(newFile, memStorage.values(), arena);
        fileStorages.addFirst(newStorage);

        memStorage.clear();
    }

    private static Iterator<Entry<MemorySegment>> subMapIterator(
            ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> map,
            MemorySegment from,
            MemorySegment to) {
        if (from == null && to == null) {
            return map.values().iterator();
        } else if (from == null) {
            return map.headMap(to, false).values().iterator();
        } else if (to == null) {
            return map.tailMap(from, true).values().iterator();
        } else {
            return map.subMap(from, true, to, false).values().iterator();
        }
    }

    private static int compare(MemorySegment segment1, MemorySegment segment2) {
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