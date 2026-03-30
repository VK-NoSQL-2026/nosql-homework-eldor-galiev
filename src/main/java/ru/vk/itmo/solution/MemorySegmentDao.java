package ru.vk.itmo.solution;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage =
            new ConcurrentSkipListMap<>(this::compare);
    private final Path path;

    public MemorySegmentDao(Config config) {
        this.path = config.basePath().resolve("dao.data");
        load();
    }

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
        save();
    }

    @Override
    public void close() {
        save();
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

    private void load() {
        if (!Files.exists(path)) {
            return;
        }
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocateDirect(8192);
            while (true) {
                if (!readFully(channel, buffer, 4)) break;
                buffer.flip();
                int keyLen = buffer.getInt();
                buffer.compact();

                if (keyLen < 0) break;

                if (!readFully(channel, buffer, keyLen)) {
                    throw new RuntimeException("Unexpected EOF while reading key");
                }
                buffer.flip();
                byte[] keyBytes = new byte[keyLen];
                buffer.get(keyBytes);
                buffer.compact();
                MemorySegment key = MemorySegment.ofArray(keyBytes);

                if (!readFully(channel, buffer, 4)) {
                    throw new RuntimeException("Unexpected EOF while reading value length");
                }
                buffer.flip();
                int valLen = buffer.getInt();
                buffer.compact();

                MemorySegment value = null;
                if (valLen >= 0) {
                    if (!readFully(channel, buffer, valLen)) {
                        throw new RuntimeException("Unexpected EOF while reading value");
                    }
                    buffer.flip();
                    byte[] valBytes = new byte[valLen];
                    buffer.get(valBytes);
                    buffer.compact();
                    value = MemorySegment.ofArray(valBytes);
                }

                Entry<MemorySegment> entry = new BaseEntry<>(key, value);
                storage.put(key, entry);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load data from " + path, e);
        }
    }

    private boolean readFully(FileChannel channel, ByteBuffer buffer, int bytes) throws IOException {
        int totalRead = 0;
        while (totalRead < bytes) {
            int read = channel.read(buffer);
            if (read == -1) return false;
            totalRead += read;
        }
        return true;
    }

    private void save() {
        try {
            Files.createDirectories(path.getParent());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create directory for " + path, e);
        }

        try (FileChannel channel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            for (Entry<MemorySegment> entry : storage.values()) {
                MemorySegment key = entry.key();
                MemorySegment value = entry.value();

                long keySize = key.byteSize();
                if (keySize > Integer.MAX_VALUE) {
                    throw new RuntimeException("Key too large: " + keySize);
                }
                writeInt(channel, (int) keySize);
                writeFully(channel, key.asByteBuffer());

                if (value == null) {
                    writeInt(channel, -1);
                } else {
                    long valSize = value.byteSize();
                    if (valSize > Integer.MAX_VALUE) {
                        throw new RuntimeException("Value too large: " + valSize);
                    }
                    writeInt(channel, (int) valSize);
                    writeFully(channel, value.asByteBuffer());
                }
            }
            channel.force(true);
        } catch (IOException e) {
            throw new RuntimeException("Failed to save data to " + path, e);
        }
    }

    private void writeInt(FileChannel channel, int value) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(value);
        buffer.flip();
        writeFully(channel, buffer);
    }

    private void writeFully(FileChannel channel, ByteBuffer buffer) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }
}