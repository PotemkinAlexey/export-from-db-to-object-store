"""Per-shard export metrics with rich log summary."""
from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from .operator import ExportFromDBToObjectStoreOperator


class ExportMetrics:
    """Collect detailed shard-level metrics."""

    def __init__(self, operator: "ExportFromDBToObjectStoreOperator"):
        self.operator = operator
        self.start_time: Optional[float] = None
        self.shards: List[Dict[str, Any]] = []

    def start(self) -> None:
        self.start_time = time.time()

    def record_shard(self, shard_index: int, rows: int, bytes_: int, duration: float) -> None:
        """Always record shard metrics — even 0-row shards."""
        self.shards.append(
            {
                "shard_index": shard_index,
                "rows": rows,
                "bytes": bytes_,
                "bytes_mb": bytes_ / 1048576,
                "duration_s": duration,
                "throughput_rows_s": rows / max(duration, 0.001),
                "throughput_mb_s": (bytes_ / 1048576) / max(duration, 0.001),
            }
        )

    def summary(self) -> Dict[str, Any]:
        if self.start_time is None:
            return {}

        end_time = time.time()
        total_dur = end_time - self.start_time

        total_rows = sum(s["rows"] for s in self.shards)
        total_bytes = sum(s["bytes"] for s in self.shards)
        total_bytes_mb = total_bytes / 1048576

        summary = {
            "start_time": self.start_time,
            "end_time": end_time,
            "duration_s": total_dur,
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "total_bytes_mb": total_bytes_mb,
            "avg_rows_s": total_rows / max(total_dur, 0.001),
            "avg_mb_s": (total_bytes / 1048576) / max(total_dur, 0.001),
            "shards": self.shards,
        }

        # ============================================================
        # SUPER-GRAFANA METRICS BLOCK (ASCII ART + ANALYTICS)
        # ============================================================

        def bar(value, max_value, width=32):
            filled = int((value / max_value) * width)
            return "█" * filled + "░" * (width - filled)

        def spark(values):
            ticks = "▁▂▃▄▅▆▇█"
            mx = max(values) or 1
            mn = min(values)
            rng = mx - mn or 1
            return "".join(ticks[int((v - mn) / rng * (len(ticks) - 1))] for v in values)

        shards = self.shards
        max_rps = max(s["throughput_rows_s"] for s in shards) or 1
        max_mbps = max(s["throughput_mb_s"] for s in shards) or 1
        avg_mbps = sum(s["throughput_mb_s"] for s in shards) / len(shards)

        slow_threshold = avg_mbps * 0.40

        visual_rows = []
        for s in shards:
            slow = "🐢" if s["throughput_mb_s"] < slow_threshold else " "
            visual_rows.append(
                f"Shard {s['shard_index']:>2} {slow}\n"
                f"  Rows: {s['rows']:,}\n"
                f"  Size: {s['bytes_mb']:,.2f} MB,  Time: {s['duration_s']:.2f}s\n"
                f"  Rows/s: {s['throughput_rows_s']:.0f}\n"
                f"  MB/s:   {s['throughput_mb_s']:.2f}\n"
                f"  RPS: {bar(s['throughput_rows_s'], max_rps)}\n"
                f"  MB/s:{bar(s['throughput_mb_s'], max_mbps)}\n"
            )

        rps_spark = spark([s["throughput_rows_s"] for s in shards])
        mb_spark = spark([s["throughput_mb_s"] for s in shards])

        variation = (max_mbps - min(s["throughput_mb_s"] for s in shards)) / max_mbps
        if variation < 0.25:
            grade = "A+  (Ultra stable 🚀🔥)"
        elif variation < 0.45:
            grade = "A   (Good stability ⚡)"
        elif variation < 0.65:
            grade = "B   (Mixed performance 📉)"
        else:
            grade = "C   (Unstable, slow shards 🐢)"

        fastest = sorted(shards, key=lambda x: x["throughput_mb_s"], reverse=True)[:3]
        slowest = sorted(shards, key=lambda x: x["throughput_mb_s"])[:3]

        self.operator.log.info(
            "\n\n"
            "───────────────────────────────────────────────────────────────\n"
            "🚀  ADVANCED EXPORT ANALYTICS 🚀   \n"
            "───────────────────────────────────────────────────────────────\n"
            f"📦 Total rows       : {total_rows:,}\n"
            f"💾 Total size       : {total_bytes_mb:.2f} MB\n"
            f"⏱  Total duration   : {total_dur:.2f} s\n"
            f"🚀 Avg speed (MB/s) : {avg_mbps:.2f}\n"
            f"📊 Avg rows/s       : {summary['avg_rows_s']:.0f}\n"
            f"📈 Stability grade   : {grade}\n"
            "───────────────────────────────────────────────────────────────\n"
            "🔥 GLOBAL SPARKLINES\n"
            f"   Rows/s: {rps_spark}\n"
            f"   MB/s  : {mb_spark}\n"
            "───────────────────────────────────────────────────────────────\n"
            "📊 PER-SHARD METRICS\n"
            + "\n".join(visual_rows)
            + "───────────────────────────────────────────────────────────────\n"
            "🏆 FASTEST SHARDS\n"
            + "\n".join([f"   🚀 Shard {s['shard_index']:>2} → {s['throughput_mb_s']:.2f} MB/s" for s in fastest])
            + "\n\n🐢 SLOWEST SHARDS\n"
            + "\n".join([f"   🐢 Shard {s['shard_index']:>2} → {s['throughput_mb_s']:.2f} MB/s" for s in slowest])
            + "\n───────────────────────────────────────────────────────────────\n"
            "🧠 PRO TIP: Speed = depends on chunk tuning + DB bandwidth.\n"
            "███████████████████████████████████████████████████████████████\n"
        )

        return summary
