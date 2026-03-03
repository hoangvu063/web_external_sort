# core_sort.py
import heapq
import struct
import os
import math
from typing import List, Any, Dict

class ExternalSortSimulator:
    def __init__(self,
                 input_path: str,
                 output_path: str,
                 chunk_size: int = 4,
                 block_size: int = 2,
                 k_way: int = 2):
        self.input_path = input_path
        self.output_path = output_path
        self.chunk_size = max(1, int(chunk_size))
        self.block_size = max(1, int(block_size))
        self.k_way = max(2, int(k_way))
        self.format = '<d'
        self.size_of_double = 8

        self.logs: List[Dict[str, Any]] = []
        self.disk_runs: List[List[float]] = []
        self.pass_stats: List[Dict[str, Any]] = []
        self._total_disk_reads = 0
        self._total_disk_writes = 0

    def _log(self, type: str, **kwargs):
        entry = {
            "type": type,
            "step": len(self.logs) + 1,
            "disk_reads": self._total_disk_reads,
            "disk_writes": self._total_disk_writes,
            # commonly used fields
            "pass_num": kwargs.get("pass_num", None),
            "run_id": kwargs.get("run_id", None),
            "value": kwargs.get("value", None),
            "flushed": kwargs.get("flushed", []),
            "heap_state": kwargs.get("heap_state", []),
            "buffers": kwargs.get("buffers", []),
            "block": kwargs.get("block", []),
            "msg": kwargs.get("msg", ""),
        }
        # attach anything else
        for k, v in kwargs.items():
            if k not in entry:
                entry[k] = v
        self.logs.append(entry)

    def _phase1(self):
        if not os.path.exists(self.input_path):
            raise FileNotFoundError(f"Input file not found: {self.input_path}")

        with open(self.input_path, 'rb') as f:
            run_idx = 0
            leftover: List[float] = []

            while True:
                chunk: List[float] = []

                if leftover:
                    take = min(self.chunk_size, len(leftover))
                    chunk.extend(leftover[:take])
                    leftover = leftover[take:]

                while len(chunk) < self.chunk_size:
                    raw = f.read(self.size_of_double * self.block_size)
                    if not raw:
                        break
                    n = len(raw) // self.size_of_double
                    vals = [round(x[0], 6) for x in struct.iter_unpack(self.format, raw)]
                    self._total_disk_reads += n

                    self._log(
                        "BUFFER_LOAD",
                        pass_num=0,
                        run_id=run_idx,
                        block=list(vals),
                        buffers=[],
                        heap_state=[],
                        msg=f"Phase 1 · Read block ({n} items) for run {run_idx}"
                    )

                    need = self.chunk_size - len(chunk)
                    if len(vals) <= need:
                        chunk.extend(vals)
                    else:
                        chunk.extend(vals[:need])
                        leftover = vals[need:]

                if not chunk:
                    break

                chunk.sort()
                self.disk_runs.append(list(chunk))
                self._total_disk_writes += len(chunk)

                self._log(
                    "RUN_CREATED",
                    pass_num=0,
                    run_id=run_idx,
                    values=list(chunk),
                    buffers=[],
                    heap_state=[],
                    msg=f"Phase 1 · Run {run_idx} created ({len(chunk)} items) and written to (virtual) disk"
                )

                run_idx += 1

        self.pass_stats.append({
            "pass": 0,
            "label": "Phase 1 – Run Generation",
            "disk_reads": self._total_disk_reads,
            "disk_writes": self._total_disk_writes,
            "runs_in": 1,
            "runs_out": len(self.disk_runs)
        })

    def _refill_block(self, run_data: List[float], pointers: List[int], r_idx: int) -> List[float]:
        start = pointers[r_idx]
        end = min(start + self.block_size, len(run_data))
        if start < end:
            block = run_data[start:end]
            pointers[r_idx] = end
            self._total_disk_reads += len(block)
            return list(block)
        return []

    def _merge_group(self, group_runs: List[List[float]], pass_num: int, group_idx: int) -> List[float]:
        num = len(group_runs)
        pointers = [0] * num
        input_buffers: List[List[float]] = [[] for _ in range(num)]
        min_heap: List[tuple] = []
        result: List[float] = []

        for i in range(num):
            block = self._refill_block(group_runs[i], pointers, i)
            if block:
                input_buffers[i].extend(block)
                self._log(
                    "BUFFER_LOAD",
                    pass_num=pass_num,
                    run_id=group_idx * self.k_way + i,
                    block=list(block),
                    buffers=[list(b) for b in input_buffers],
                    heap_state=[h[0] for h in min_heap],
                    msg=f"Pass {pass_num} · Load block into Buffer[{i}]"
                )

        for i in range(num):
            if input_buffers[i]:
                val = input_buffers[i].pop(0)
                heapq.heappush(min_heap, (val, i))
                heap_state_snapshot = [h[0] for h in min_heap] if min_heap else []
                self._log(
                    "HEAP_PUSH",
                    pass_num=pass_num,
                    run_id=group_idx * self.k_way + i,
                    value=val,
                    buffers=[list(b) for b in input_buffers],
                    heap_state=heap_state_snapshot,
                    msg=f"Pass {pass_num} · Push {val} from Buffer[{i}]"
                )

        output_buffer: List[float] = []

        while min_heap:
            val, local_idx = heapq.heappop(min_heap)

            self._log(
                "HEAP_POP",
                pass_num=pass_num,
                run_id=group_idx * self.k_way + local_idx,
                value=val,
                buffers=[list(b) for b in input_buffers],
                heap_state=[h[0] for h in min_heap],
                msg=f"Pass {pass_num} · Pop {val} from heap → output buffer"
            )

            output_buffer.append(val)
            result.append(val)

            if len(output_buffer) >= self.block_size:
                flushed = list(output_buffer)
                self._total_disk_writes += len(flushed)
                self._log(
                    "OUTPUT_FLUSH",
                    pass_num=pass_num,
                    run_id=group_idx * self.k_way + local_idx,
                    flushed=flushed,
                    buffers=[list(b) for b in input_buffers],
                    heap_state=[h[0] for h in min_heap],
                    is_final_flush=False,
                    msg=f"Pass {pass_num} · Flush {len(flushed)} items to (virtual) disk"
                )
                output_buffer = []

            if not input_buffers[local_idx]:
                block = self._refill_block(group_runs[local_idx], pointers, local_idx)
                if block:
                    input_buffers[local_idx].extend(block)
                    self._log(
                        "BUFFER_LOAD",
                        pass_num=pass_num,
                        run_id=group_idx * self.k_way + local_idx,
                        block=list(block),
                        buffers=[list(b) for b in input_buffers],
                        heap_state=[h[0] for h in min_heap],
                        msg=f"Pass {pass_num} · Refill Buffer[{local_idx}]"
                    )

            if input_buffers[local_idx]:
                nxt = input_buffers[local_idx].pop(0)
                heapq.heappush(min_heap, (nxt, local_idx))
                self._log(
                    "HEAP_PUSH",
                    pass_num=pass_num,
                    run_id=group_idx * self.k_way + local_idx,
                    value=nxt,
                    buffers=[list(b) for b in input_buffers],
                    heap_state=[h[0] for h in min_heap],
                    msg=f"Pass {pass_num} · Push {nxt} from Buffer[{local_idx}]"
                )

        if output_buffer:
            flushed = list(output_buffer)
            self._total_disk_writes += len(flushed)
            self._log(
                "OUTPUT_FLUSH",
                pass_num=pass_num,
                run_id=-1,
                flushed=flushed,
                buffers=[list(b) for b in input_buffers],
                heap_state=[],
                is_final_flush=True,
                msg=f"Pass {pass_num} · Final flush {len(flushed)} items"
            )

        return result

    def _phase2(self):
        current_runs = [list(r) for r in self.disk_runs]
        pass_num = 1

        if len(current_runs) <= 1:
            if current_runs:
                final = current_runs[0]
                with open(self.output_path, 'wb') as out_f:
                    for v in final:
                        out_f.write(struct.pack(self.format, v))
                self._total_disk_writes += len(final)

            self._log(
                "SORT_COMPLETE",
                pass_num=0,
                total_elements=len(current_runs[0]) if current_runs else 0,
                total_passes=0,
                total_disk_reads=self._total_disk_reads,
                total_disk_writes=self._total_disk_writes,
                buffers=[],
                heap_state=[],
                msg="Sort complete (no merge needed)"
            )
            return

        while len(current_runs) > 1:
            reads_before = self._total_disk_reads
            writes_before = self._total_disk_writes

            total_groups = math.ceil(len(current_runs) / self.k_way)
            self._log(
                "PASS_START",
                pass_num=pass_num,
                num_runs=len(current_runs),
                k_way=self.k_way,
                total_groups=total_groups,
                buffers=[],
                heap_state=[],
                msg=f"Pass {pass_num} · Start merging {len(current_runs)} runs with {self.k_way}-way groups"
            )

            next_runs: List[List[float]] = []

            for g in range(total_groups):
                group = current_runs[g * self.k_way: (g + 1) * self.k_way]
                merged = self._merge_group(group, pass_num, g)
                next_runs.append(merged)

                self._log(
                    "PASS_GROUP_DONE",
                    pass_num=pass_num,
                    group_idx=g,
                    result_size=len(merged),
                    buffers=[],
                    heap_state=[],
                    msg=f"Pass {pass_num} · Group {g} merged → {len(merged)} items"
                )

            self.pass_stats.append({
                "pass": pass_num,
                "label": f"Pass {pass_num} – {self.k_way}-way Merge",
                "disk_reads": self._total_disk_reads - reads_before,
                "disk_writes": self._total_disk_writes - writes_before,
                "runs_in": len(current_runs),
                "runs_out": len(next_runs)
            })

            current_runs = next_runs
            pass_num += 1

        final = current_runs[0] if current_runs else []
        with open(self.output_path, 'wb') as out_f:
            for v in final:
                out_f.write(struct.pack(self.format, v))
        self._total_disk_writes += len(final)

        self._log(
            "SORT_COMPLETE",
            pass_num=pass_num - 1,
            total_elements=len(final),
            total_passes=pass_num - 1,
            total_disk_reads=self._total_disk_reads,
            total_disk_writes=self._total_disk_writes,
            buffers=[],
            heap_state=[],
            msg=f"✓ Sort complete · {len(final)} items · {pass_num-1} passes"
        )

    def run_simulation(self) -> Dict[str, Any]:
        self.logs = []
        self.disk_runs = []
        self.pass_stats = []
        self._total_disk_reads = 0
        self._total_disk_writes = 0

        self._phase1()
        self._phase2()

        summary = {
            "total_steps": len(self.logs),
            "total_runs": len(self.disk_runs),
            "total_disk_reads": self._total_disk_reads,
            "total_disk_writes": self._total_disk_writes,
            "k_way": self.k_way,
            "chunk_size": self.chunk_size,
            "block_size": self.block_size,
        }

        return {
            "logs": self.logs,
            "pass_stats": self.pass_stats,
            "summary": summary
        }