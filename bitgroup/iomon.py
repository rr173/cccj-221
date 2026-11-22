"""可观察的 I/O 与内存计数器。

所有物理页读取、缓存命中、临时位图驻留量都汇总在这里，
引擎每轮查询前打快照，查询后即可给出真实观测值。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict


@dataclass
class IOMonitor:
    # 物理读取
    page_reads: int = 0
    bytes_read: int = 0
    # 物理写回（临时位图落盘）
    page_writes: int = 0
    bytes_written: int = 0
    # LRU 页缓存
    cache_hits: int = 0
    cache_misses: int = 0
    cache_evictions: int = 0
    cache_resident_bytes: int = 0
    cache_resident_peak: int = 0
    # 临时位图（逻辑大小按未压缩页计，驻留量按当前在缓存中的脏/净页计）
    temp_live_logical: int = 0
    temp_live_logical_peak: int = 0
    temp_live_count: int = 0
    temp_live_count_peak: int = 0
    temp_resident_bytes: int = 0
    temp_resident_peak: int = 0
    # 执行期处理的子行计数（lift 阶段逐分片统计）
    child_rows_processed: int = 0
    # 打开过的 posting 位图文件数（稀疏条件读取面的观测）
    posting_files_opened: int = 0
    posting_pages_touched: int = 0
    extra: Dict[str, int] = field(default_factory=dict)

    def snapshot(self) -> Dict[str, int]:
        data = {
            "page_reads": self.page_reads,
            "bytes_read": self.bytes_read,
            "page_writes": self.page_writes,
            "bytes_written": self.bytes_written,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "cache_evictions": self.cache_evictions,
            "cache_resident_bytes": self.cache_resident_bytes,
            "cache_resident_peak": self.cache_resident_peak,
            "temp_live_logical_bytes": self.temp_live_logical,
            "temp_live_logical_peak": self.temp_live_logical_peak,
            "temp_live_bitmaps": self.temp_live_count,
            "temp_live_bitmaps_peak": self.temp_live_count_peak,
            "temp_resident_bytes": self.temp_resident_bytes,
            "temp_resident_peak": self.temp_resident_peak,
            "child_rows_processed": self.child_rows_processed,
            "posting_files_opened": self.posting_files_opened,
            "posting_pages_touched": self.posting_pages_touched,
        }
        data.update(self.extra)
        return data

    def delta(self, before: Dict[str, int]) -> Dict[str, int]:
        now = self.snapshot()
        out = {}
        peak_keys = {
            "cache_resident_peak",
            "temp_live_logical_peak",
            "temp_live_bitmaps_peak",
            "temp_resident_peak",
        }
        for k, v in now.items():
            old = before.get(k, 0)
            if k in peak_keys:
                # 查询开始前已 reset_peaks，这里的峰即本次查询窗口内的真实峰
                out[k] = v
            else:
                out[k] = v - old
        return out

    def reset_peaks(self) -> None:
        """把峰值基线归零（累计计数器不变），用于单次查询的精确峰值观测。"""
        self.cache_resident_peak = 0
        self.temp_live_logical_peak = 0
        self.temp_live_count_peak = 0
        self.temp_resident_peak = 0
