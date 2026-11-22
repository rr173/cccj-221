"""位图层直接单元测试：RLE 往返、LRU 驱逐落盘、临时位图、分页边界、身份映射。"""

import os
import shutil
import tempfile
import unittest

from bitgroup.iomon import IOMonitor
from bitgroup.pages import (
    PAGE_BITS,
    BitmapStore,
    BitmapWriter,
    shards_for,
)
from bitgroup.ident import fnv1a64
from bitgroup.engine import Engine
from bitgroup.importer import Importer


def make_words(set_bits):
    w = [0] * 1024
    for b in set_bits:
        w[b >> 6] |= 1 << (b & 63)
    return w


class PagesCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="bgpage_")
        self.mon = IOMonitor()
        self.store = BitmapStore(self.tmp, self.mon, 4 * PAGE_BITS * 8)

    def tearDown(self):
        self.store.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_rle_roundtrip_and_empty(self):
        w = BitmapWriter(os.path.join(self.tmp, "a.bit"))
        full_one = [0xFFFFFFFFFFFFFFFF] * 1024
        sparse = make_words([0, 63, 65535])
        w.write_page(None)          # EMPTY
        w.write_page(full_one)      # 全 1 -> RLE
        w.write_page(sparse)        # 稀疏 -> RAW/RLE
        w.finish(3)
        bf = self.store.open_bitmap("a.bit", 3, count_files=False)
        self.assertIsNone(bf.read_page(0))
        p1 = bf.read_page(1)
        self.assertEqual(p1, full_one)
        p2 = bf.read_page(2)
        for b in (0, 63, 65535):
            self.assertTrue(p2[b >> 6] & (1 << (b & 63)))
        self.assertEqual(sum(x.bit_count() for x in p2), 3)

    def test_temp_bitmap_eviction_and_reload(self):
        # 预算只够 3 页，强制临时页驱逐到 spool 后再读回
        small = BitmapStore(os.path.join(self.tmp, "s"), IOMonitor(),
                            3 * 8192)
        try:
            tb = small.new_temp(6, "evict")
            for i in range(6):
                tb.set_bit(i * PAGE_BITS + 7)
            # 全部读回应一致
            for i in range(6):
                self.assertTrue(tb.get_bit(i * PAGE_BITS + 7))
                self.assertFalse(tb.get_bit(i * PAGE_BITS + 8))
            self.assertGreater(small.monitor.page_writes, 0)
            tb.release()
        finally:
            small.close()

    def test_temp_persist(self):
        tb = self.store.new_temp(2, "persist")
        tb.set_bit(0)
        tb.set_bit(PAGE_BITS + 100)
        tb.persist(os.path.join(self.tmp, "out.bit"))
        tb.release()
        bf = self.store.open_bitmap("out.bit", 2, count_files=False)
        self.assertTrue(bf.read_page(0)[0] & 1)
        self.assertTrue(bf.read_page(1)[100 >> 6] & (1 << (100 & 63)))

    def test_shards(self):
        self.assertEqual(shards_for(0), 0)
        self.assertEqual(shards_for(1), 1)
        self.assertEqual(shards_for(PAGE_BITS), 1)
        self.assertEqual(shards_for(PAGE_BITS + 1), 2)

    def test_peak_counters_monotonic(self):
        tb1 = self.store.new_temp(4, "a")
        tb2 = self.store.new_temp(4, "b")
        tb1.set_bit(0)
        tb2.set_bit(1)
        peak = self.mon.temp_live_logical_peak
        self.assertGreaterEqual(peak, 8 * 8192)
        tb1.release()
        # 峰值不回落
        self.assertEqual(self.mon.temp_live_logical_peak, peak)
        tb2.release()


class IdentityCase(unittest.TestCase):
    def test_hash_distribution_and_reversibility(self):
        # 同一字符串哈希稳定，不同字符串大量抽样几乎不撞
        self.assertEqual(fnv1a64(b"abc"), fnv1a64(b"abc"))
        vals = {fnv1a64(f"id-{i}".encode()) for i in range(10000)}
        self.assertEqual(len(vals), 10000)

    def test_childless_import(self):
        tmp = tempfile.mkdtemp()
        try:
            p = os.path.join(tmp, "p.jsonl")
            with open(p, "w") as fh:
                fh.write('{"entity_id":"A","v":1}\n')
                fh.write('{"entity_id":"B","v":null}\n')
            idx = os.path.join(tmp, "idx")
            # 声明子表 schema 但不提供子表文件
            Importer(idx, {"v": "int64"}, {"z": "int64"}).run(p, None)
            with Engine(idx) as eng:
                self.assertEqual(eng.n_children, 0)
                ast = {"op": "exists", "arg": {
                    "op": "cmp", "domain": "child", "attr": "z",
                    "op_kind": "eq", "value": 1}}
                r = eng.query(ast)
                # 无子女 -> exists 恒 false
                self.assertEqual(r.counts, {"true": 0, "unknown": 0, "false": 2})
                c0 = {"op": "count_where", "k": 0, "arg": ast["arg"]}
                self.assertEqual(eng.query(c0).counts["true"], 2)
                # 身份可逆
                self.assertEqual(eng.ident.entity_id_at(1), "B")
                self.assertEqual(eng.ident.pid_of("A"), 0)
        finally:
            shutil.rmtree(tmp)


if __name__ == "__main__":
    unittest.main()
