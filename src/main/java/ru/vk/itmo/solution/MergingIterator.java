package ru.vk.itmo.solution;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

import static ru.vk.itmo.solution.MemorySegmentDao.COMPARATOR;

public class MergingIterator implements Iterator<Entry<MemorySegment>> {
    private final PriorityQueue<SourceEntry> heap;
    private Entry<MemorySegment> nextEntry;

    public MergingIterator(List<Iterator<Entry<MemorySegment>>> iterators) {
        this.heap = new PriorityQueue<>((a, b) -> {
            int cmp = COMPARATOR.compare(a.entry.key(), b.entry.key());
            if (cmp != 0) return cmp;
            return Integer.compare(a.sourceId, b.sourceId);
        });

        for (int i = 0; i < iterators.size(); i++) {
            addIfHasNext(iterators.get(i), i);
        }
    }

    private void addIfHasNext(Iterator<Entry<MemorySegment>> iter, int sourceId) {
        if (iter.hasNext()) {
            heap.offer(new SourceEntry(iter.next(), iter, sourceId));
        }
    }

    @Override
    public boolean hasNext() {
        if (nextEntry != null) {
            return true;
        }

        while (!heap.isEmpty()) {
            SourceEntry current = heap.poll();
            Entry<MemorySegment> currentEntry = current.entry;

            while (!heap.isEmpty()) {
                SourceEntry next = heap.peek();
                if (COMPARATOR.compare(currentEntry.key(), next.entry.key()) != 0) {
                    break;
                }
                heap.poll();
                addIfHasNext(next.iterator, next.sourceId);
            }

            addIfHasNext(current.iterator, current.sourceId);

            if (currentEntry.value() != null) {
                nextEntry = currentEntry;
                return true;
            }
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

    private static final class SourceEntry {
        final Entry<MemorySegment> entry;
        final Iterator<Entry<MemorySegment>> iterator;
        final int sourceId;

        private SourceEntry(Entry<MemorySegment> entry, Iterator<Entry<MemorySegment>> iterator, int sourceId) {
            this.entry = entry;
            this.iterator = iterator;
            this.sourceId = sourceId;
        }
    }
}