package ru.vk.itmo;

import ru.vk.itmo.test.DaoFactory;

import java.io.IOException;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReverseIteratorTest extends BaseTest {

    @DaoTest(stage = 6)
    void descendingEmpty(Dao<String, Entry<String>> dao) {
        Iterator<Entry<String>> desc = dao.descendingGet(null, null);
        assertFalse(desc.hasNext());
    }

    @DaoTest(stage = 6)
    void descendingSingleEntry(Dao<String, Entry<String>> dao) {
        dao.upsert(entry("key", "value"));
        Iterator<Entry<String>> desc = dao.descendingGet(null, null);
        assertTrue(desc.hasNext());
        assertSame(desc.next(), entry("key", "value"));
        assertFalse(desc.hasNext());
    }

    @DaoTest(stage = 6)
    void descendingBasicOrder(Dao<String, Entry<String>> dao) {
        dao.upsert(entry("b", "1"));
        dao.upsert(entry("a", "2"));
        dao.upsert(entry("d", "3"));
        dao.upsert(entry("c", "4"));

        Iterator<Entry<String>> asc = dao.all();
        assertSame(asc, entry("a", "2"), entry("b", "1"), entry("c", "4"), entry("d", "3"));

        Iterator<Entry<String>> desc = dao.descendingGet(null, null);
        assertSame(desc, entry("d", "3"), entry("c", "4"), entry("b", "1"), entry("a", "2"));
    }

    @DaoTest(stage = 6)
    void descendingRangeWithBounds(Dao<String, Entry<String>> dao) {
        dao.upsert(entry("a", "1"));
        dao.upsert(entry("b", "2"));
        dao.upsert(entry("c", "3"));
        dao.upsert(entry("d", "4"));
        dao.upsert(entry("e", "5"));

        Iterator<Entry<String>> desc = dao.descendingGet("b", "e");
        assertSame(desc, entry("d", "4"), entry("c", "3"), entry("b", "2"));
    }

    @DaoTest(stage = 6)
    void descendingFullRangeExcludesEnd(Dao<String, Entry<String>> dao) {
        dao.upsert(entry("a", "1"));
        dao.upsert(entry("b", "2"));
        dao.upsert(entry("c", "3"));

        Iterator<Entry<String>> desc = dao.descendingGet("a", "c");
        assertSame(desc, entry("b", "2"), entry("a", "1"));
    }

    @DaoTest(stage = 6)
    void descendingSkipsDeletes(Dao<String, Entry<String>> dao) {
        dao.upsert(entry("a", "1"));
        dao.upsert(entry("b", "2"));
        dao.upsert(entry("c", "3"));
        dao.upsert(entry("b", null));

        Iterator<Entry<String>> asc = dao.all();
        assertSame(asc, entry("a", "1"), entry("c", "3"));

        Iterator<Entry<String>> desc = dao.descendingGet(null, null);
        assertSame(desc, entry("c", "3"), entry("a", "1"));
    }

    @DaoTest(stage = 6)
    void descendingAllDeleted(Dao<String, Entry<String>> dao) {
        dao.upsert(entry("a", "1"));
        dao.upsert(entry("b", "2"));
        dao.upsert(entry("a", null));
        dao.upsert(entry("b", null));

        Iterator<Entry<String>> desc = dao.descendingGet(null, null);
        assertFalse(desc.hasNext());
    }

    @DaoTest(stage = 6)
    void descendingAfterFlush(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("x", "10"));
        dao.upsert(entry("y", "20"));
        dao.upsert(entry("z", "30"));
        dao.flush();

        dao.upsert(entry("w", "40"));
        dao.upsert(entry("v", "50"));

        Iterator<Entry<String>> desc = dao.descendingGet(null, null);
        assertSame(desc,
                entry("z", "30"),
                entry("y", "20"),
                entry("x", "10"),
                entry("w", "40"),
                entry("v", "50")
        );
    }

    @DaoTest(stage = 6)
    void descendingAfterCloseAndReopen(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("p", "1"));
        dao.upsert(entry("q", "2"));
        dao.upsert(entry("r", "3"));
        dao.close();

        Dao<String, Entry<String>> reopened = DaoFactory.Factory.reopen(dao);
        Iterator<Entry<String>> desc = reopened.descendingGet(null, null);
        assertSame(desc,
                entry("r", "3"),
                entry("q", "2"),
                entry("p", "1")
        );
        reopened.close();
    }

    @DaoTest(stage = 6)
    void descendingAfterCompact(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("k1", "v1"));
        dao.upsert(entry("k2", "v2"));
        dao.upsert(entry("k3", "v3"));
        dao.flush();

        dao.upsert(entry("k2", "updated"));
        dao.upsert(entry("k4", "v4"));
        dao.upsert(entry("k3", null));
        dao.flush();

        dao.compact();
        dao.close();

        Dao<String, Entry<String>> reopened = DaoFactory.Factory.reopen(dao);
        Iterator<Entry<String>> desc = reopened.descendingGet(null, null);
        assertSame(desc,
                entry("k4", "v4"),
                entry("k2", "updated"),
                entry("k1", "v1")
        );
        reopened.close();
    }

    @DaoTest(stage = 6)
    void descendingMixedMemoryAndDisk(Dao<String, Entry<String>> dao) throws IOException {
        dao.upsert(entry("disk1", "old1"));
        dao.upsert(entry("disk2", "old2"));
        dao.close();

        Dao<String, Entry<String>> reopened = DaoFactory.Factory.reopen(dao);
        reopened.upsert(entry("mem1", "new1"));
        reopened.upsert(entry("disk2", "overwritten"));
        reopened.upsert(entry("mem2", "new2"));
        reopened.upsert(entry("disk1", null));

        Iterator<Entry<String>> desc = reopened.descendingGet(null, null);
        assertSame(desc,
                entry("mem2", "new2"),
                entry("mem1", "new1"),
                entry("disk2", "overwritten")
        );
        reopened.close();
    }

    @DaoTest(stage = 6)
    void descendingRangeWithDeletes(Dao<String, Entry<String>> dao) {
        dao.upsert(entry("a", "1"));
        dao.upsert(entry("b", "2"));
        dao.upsert(entry("c", "3"));
        dao.upsert(entry("d", "4"));
        dao.upsert(entry("e", "5"));
        dao.upsert(entry("b", null));
        dao.upsert(entry("c", null));

        Iterator<Entry<String>> desc = dao.descendingGet("a", "e");
        assertSame(desc, entry("d", "4"), entry("a", "1"));
    }

    @DaoTest(stage = 6)
    void descendingFromNull(Dao<String, Entry<String>> dao) {
        dao.upsert(entry("a", "1"));
        dao.upsert(entry("b", "2"));
        dao.upsert(entry("c", "3"));

        Iterator<Entry<String>> desc = dao.descendingGet(null, "c");
        assertSame(desc, entry("b", "2"), entry("a", "1"));
    }

    @DaoTest(stage = 6)
    void descendingToNull(Dao<String, Entry<String>> dao) {
        dao.upsert(entry("a", "1"));
        dao.upsert(entry("b", "2"));
        dao.upsert(entry("c", "3"));

        Iterator<Entry<String>> desc = dao.descendingGet("b", null);
        assertSame(desc, entry("c", "3"), entry("b", "2"));
    }
}