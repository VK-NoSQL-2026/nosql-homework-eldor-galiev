package ru.vk.itmo.solution;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

import static ru.vk.itmo.solution.MemorySegmentDao.COMPARATOR;

public class FileStorage {
    private final Path path;
    private final MemorySegment mappedSegment;
    private final int entryCount;
    private final long[] keyOffsets;
    private final int[] keyLengths;
    private final long[] valueOffsets;
    private final int[] valueLengths;


    public FileStorage(Path path, Arena arena) throws IOException {
        this.path = path;
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long fileSize = Files.size(path);

            if (fileSize == 0) {
                this.mappedSegment = null;
                this.entryCount = 0;
                this.keyOffsets = new long[0];
                this.keyLengths = new int[0];
                this.valueOffsets = new long[0];
                this.valueLengths = new int[0];
                return;
            }

            this.mappedSegment = channel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    fileSize,
                    arena
            );

            List<Long> keyOffsetsList = new ArrayList<>();
            List<Integer> keyLengthsList = new ArrayList<>();
            List<Long> valueOffsetsList = new ArrayList<>();
            List<Integer> valueLengthsList = new ArrayList<>();

            long position = 0;
            while (position < mappedSegment.byteSize()) {
                if (position + 4 > mappedSegment.byteSize()) break;
                int keyLen = mappedSegment.get(ValueLayout.JAVA_INT_UNALIGNED, position);
                if (keyLen < 0) break;
                long keyStart = position + 4;
                long keyEnd = keyStart + keyLen;
                if (keyEnd > mappedSegment.byteSize()) break;

                keyOffsetsList.add(keyStart);
                keyLengthsList.add(keyLen);

                if (keyEnd + 4 > mappedSegment.byteSize()) break;
                int valLen = mappedSegment.get(ValueLayout.JAVA_INT_UNALIGNED, keyEnd);

                if (valLen >= 0) {
                    long valueStart = keyEnd + 4;
                    if (valueStart + valLen > mappedSegment.byteSize()) break;
                    valueOffsetsList.add(valueStart);
                    valueLengthsList.add(valLen);
                    position = valueStart + valLen;
                } else {
                    valueOffsetsList.add(-1L);
                    valueLengthsList.add(0);
                    position = keyEnd + 4;
                }
            }

            this.entryCount = keyOffsetsList.size();
            this.keyOffsets = new long[entryCount];
            this.keyLengths = new int[entryCount];
            this.valueOffsets = new long[entryCount];
            this.valueLengths = new int[entryCount];

            for (int i = 0; i < entryCount; i++) {
                keyOffsets[i] = keyOffsetsList.get(i);
                keyLengths[i] = keyLengthsList.get(i);
                valueOffsets[i] = valueOffsetsList.get(i);
                valueLengths[i] = valueLengthsList.get(i);
            }
        }
    }

    public FileStorage(Path path, Collection<Entry<MemorySegment>> entries, Arena arena) throws IOException {
        this.path = path;

        if (entries.isEmpty()) {
            Files.createFile(path);
            this.mappedSegment = null;
            this.entryCount = 0;
            this.keyOffsets = new long[0];
            this.keyLengths = new int[0];
            this.valueOffsets = new long[0];
            this.valueLengths = new int[0];
            return;
        }

        List<Entry<MemorySegment>> sorted = new ArrayList<>(entries);
        sorted.sort(Comparator.comparing(Entry::key, COMPARATOR));

        this.entryCount = sorted.size();
        this.keyOffsets = new long[entryCount];
        this.keyLengths = new int[entryCount];
        this.valueOffsets = new long[entryCount];
        this.valueLengths = new int[entryCount];

        long dataSize = 0;
        for (Entry<MemorySegment> entry : sorted) {
            dataSize += 4 + entry.key().byteSize();
            MemorySegment value = entry.value();
            if (value == null) {
                dataSize += 4;
            } else {
                dataSize += 4 + value.byteSize();
            }
        }

        try (FileChannel channel = FileChannel.open(
                path,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ)) {

            MemorySegment fileSegment = channel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    dataSize,
                    Arena.ofConfined()
            );

            long position = 0;
            for (int i = 0; i < entryCount; i++) {
                Entry<MemorySegment> entry = sorted.get(i);
                MemorySegment key = entry.key();
                MemorySegment value = entry.value();

                fileSegment.set(ValueLayout.JAVA_INT_UNALIGNED, position, (int) key.byteSize());
                position += 4;

                MemorySegment.copy(key, 0, fileSegment, position, key.byteSize());
                keyOffsets[i] = position;
                keyLengths[i] = (int) key.byteSize();
                position += key.byteSize();

                if (value == null) {
                    fileSegment.set(ValueLayout.JAVA_INT_UNALIGNED, position, -1);
                    valueOffsets[i] = -1;
                    valueLengths[i] = 0;
                    position += 4;
                } else {
                    fileSegment.set(ValueLayout.JAVA_INT_UNALIGNED, position, (int) value.byteSize());
                    position += 4;

                    MemorySegment.copy(value, 0, fileSegment, position, value.byteSize());
                    valueOffsets[i] = position;
                    valueLengths[i] = (int) value.byteSize();
                    position += value.byteSize();
                }
            }

            channel.force(true);
        }

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            long fileSize = Files.size(path);
            if (fileSize > 0) {
                this.mappedSegment = channel.map(
                        FileChannel.MapMode.READ_ONLY,
                        0,
                        fileSize,
                        arena
                );
            } else {
                this.mappedSegment = null;
            }
        }
    }

    public Path getPath() {
        return path;
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        if (entryCount == 0) {
            return null;
        }
        int idx = binarySearch(key);
        if (idx < 0) return null;
        return readEntryAtIndex(idx);
    }

    public Iterator<Entry<MemorySegment>> rangeIterator(MemorySegment from, MemorySegment to) {
        if (entryCount == 0) {
            return Collections.emptyIterator();
        }

        int startIdx = 0;
        if (from != null) {
            int idx = binarySearch(from);
            startIdx = (idx >= 0) ? idx : -idx - 1;
        }
        int endIdx = entryCount;
        if (to != null) {
            int idx = binarySearch(to);
            endIdx = (idx >= 0) ? idx : -idx - 1;
        }
        return new FileRangeIterator(startIdx, endIdx);
    }

    private int binarySearch(MemorySegment key) {
        int low = 0;
        int high = entryCount - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            MemorySegment midKey = mappedSegment.asSlice(keyOffsets[mid], keyLengths[mid]);
            int cmp = COMPARATOR.compare(midKey, key);

            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -(low + 1);
    }

    private Entry<MemorySegment> readEntryAtIndex(int idx) {
        MemorySegment key = mappedSegment.asSlice(keyOffsets[idx], keyLengths[idx]);

        if (valueOffsets[idx] == -1) {
            return new BaseEntry<>(key, null);
        }

        MemorySegment value = mappedSegment.asSlice(valueOffsets[idx], valueLengths[idx]);
        return new BaseEntry<>(key, value);
    }

    private final class FileRangeIterator implements Iterator<Entry<MemorySegment>> {
        private int currentIdx;
        private final int endIdx;
        private Entry<MemorySegment> nextEntry;

        FileRangeIterator(int startIdx, int endIdx) {
            this.currentIdx = startIdx;
            this.endIdx = endIdx;
        }

        @Override
        public boolean hasNext() {
            if (nextEntry != null) {
                return true;
            }

            while (currentIdx < endIdx) {
                Entry<MemorySegment> entry = readEntryAtIndex(currentIdx);
                currentIdx++;
                nextEntry = entry;
                return true;
            }
            return false;
        }

        @Override
        public Entry<MemorySegment> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Entry<MemorySegment> result = nextEntry;
            nextEntry = null;
            return result;
        }
    }
}