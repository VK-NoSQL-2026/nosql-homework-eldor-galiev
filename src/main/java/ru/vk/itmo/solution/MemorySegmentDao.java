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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final String FILE_PREFIX = "sstable_";
    static final Comparator<MemorySegment> COMPARATOR = MemorySegmentDao::compare;

    private final Path baseDir;
    private final CopyOnWriteArrayList<FileStorage> fileStorages;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicInteger nextFileId;
    private final Arena arena;
    private final long flushThresholdBytes;

    private volatile ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> activeMemTable;
    private volatile ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> flushingMemTable;
    private final AtomicLong memTableSize = new AtomicLong(0);
    private final ExecutorService backgroundExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "dao-background");
        t.setDaemon(true);
        return t;
    });

    public MemorySegmentDao(Config config) {
        this.baseDir = config.basePath();
        this.flushThresholdBytes = config.flushThresholdBytes();
        this.activeMemTable = new ConcurrentSkipListMap<>(COMPARATOR);
        this.flushingMemTable = null;
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

        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();

        iterators.add(subMapIterator(activeMemTable, from, to));

        ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> flushing = this.flushingMemTable;
        if (flushing != null) {
            iterators.add(subMapIterator(flushing, from, to));
        }

        for (FileStorage fs : fileStorages) {
            iterators.add(fs.rangeIterator(from, to));
        }

        return new MergingIterator(iterators);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (key == null || closed.get()) {
            return null;
        }

        Entry<MemorySegment> entry = activeMemTable.get(key);
        if (entry != null) {
            return entry.value() == null ? null : entry;
        }

        ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> flushing = this.flushingMemTable;
        if (flushing != null) {
            entry = flushing.get(key);
            if (entry != null) {
                return entry.value() == null ? null : entry;
            }
        }

        for (FileStorage fs : fileStorages) {
            entry = fs.get(key);
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

        long entrySize = entry.key().byteSize();
        if (entry.value() != null) {
            entrySize += entry.value().byteSize();
        }

        long finalEntrySize = entrySize;
        activeMemTable.compute(entry.key(), (k, old) -> {
            long oldSize = 0;
            if (old != null) {
                oldSize = old.key().byteSize();
                if (old.value() != null) {
                    oldSize += old.value().byteSize();
                }
            }
            memTableSize.addAndGet(finalEntrySize - oldSize);
            return entry;
        });

        if (flushThresholdBytes > 0 && memTableSize.get() >= flushThresholdBytes) {
            triggerAutoFlush();
        }
    }

    private synchronized void triggerAutoFlush() {
        if (flushingMemTable != null) {
            throw new IllegalStateException("Flush already in progress, write throttling is required");
        }
        scheduleFlushInternal();
    }

    @Override
    public void flush() {
        if (closed.get()) {
            return;
        }
        synchronized (this) {
            if (flushingMemTable != null) {
                return;
            }
            if (activeMemTable.isEmpty()) {
                return;
            }
            scheduleFlushInternal();
        }
    }

    private void scheduleFlushInternal() {
        flushingMemTable = activeMemTable;
        activeMemTable = new ConcurrentSkipListMap<>(COMPARATOR);
        memTableSize.set(0);

        ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> tableToFlush = flushingMemTable;
        backgroundExecutor.execute(() -> flushMemTableSync(tableToFlush));
    }

    private void flushMemTableSync(ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> table) {
        try {
            if (table.isEmpty()) {
                return;
            }
            int fileId = nextFileId.incrementAndGet();
            Path newFile = baseDir.resolve(String.format(FILE_PREFIX + "%05d", fileId));
            FileStorage newStorage = new FileStorage(newFile, table.values(), arena);
            fileStorages.add(0, newStorage);
        } catch (IOException e) {
            throw new RuntimeException("Flush failed", e);
        } finally {
            flushingMemTable = null;
        }
    }

    @Override
    public void compact() {
        if (closed.get()) {
            throw new IllegalStateException("DAO is closed");
        }
        backgroundExecutor.execute(() -> {
            try {
                List<FileStorage> currentFiles = new ArrayList<>(fileStorages);
                if (currentFiles.isEmpty()) {
                    return;
                }

                List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
                for (FileStorage fs : currentFiles) {
                    iterators.add(fs.rangeIterator(null, null));
                }

                MergingIterator mergingIterator = new MergingIterator(iterators);
                List<Entry<MemorySegment>> aliveEntries = new ArrayList<>();
                while (mergingIterator.hasNext()) {
                    aliveEntries.add(mergingIterator.next());
                }

                if (!aliveEntries.isEmpty()) {
                    int newFileId = nextFileId.incrementAndGet();
                    Path newFile = baseDir.resolve(String.format(FILE_PREFIX + "%05d", newFileId));
                    FileStorage newStorage = new FileStorage(newFile, aliveEntries, arena);
                    fileStorages.add(0, newStorage);
                }

                for (FileStorage fs : currentFiles) {
                    Files.deleteIfExists(fs.getPath());
                }
                fileStorages.removeAll(currentFiles);
            } catch (IOException e) {
                throw new RuntimeException("Compact failed", e);
            }
        });
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        backgroundExecutor.shutdown();
        try {
            while (!backgroundExecutor.awaitTermination(1, TimeUnit.SECONDS)) {
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        try {
            if (!activeMemTable.isEmpty()) {
                int fileId = nextFileId.incrementAndGet();
                Path newFile = baseDir.resolve(String.format(FILE_PREFIX + "%05d", fileId));
                FileStorage newStorage = new FileStorage(newFile, activeMemTable.values(), arena);
                fileStorages.add(0, newStorage);
                activeMemTable.clear();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to finalize DAO state", e);
        }

        arena.close();
        fileStorages.clear();
        activeMemTable = null;
        flushingMemTable = null;
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