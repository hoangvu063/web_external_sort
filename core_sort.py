import heapq
import struct
import os
import math
import tempfile
import shutil
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

        # Tạo thư mục tạm để chứa các file run (chunk)
        self.temp_dir = tempfile.mkdtemp(prefix="ext_sort_real_")
        
        self.logs: List[Dict[str, Any]] = []
        # Bây giờ disk_runs sẽ chứa ĐƯỜNG DẪN file (str) thay vì mảng dữ liệu
        self.disk_runs: List[str] = [] 
        self.pass_stats: List[Dict[str, Any]] = []
        self._total_disk_reads = 0
        self._total_disk_writes = 0

    def cleanup(self):
        """Xóa thư mục tạm sau khi chạy xong"""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _log(self, type: str, **kwargs):
        # Hàm log giữ nguyên để tương thích frontend
        entry = {
            "type": type,
            "step": len(self.logs) + 1,
            "disk_reads": self._total_disk_reads,
            "disk_writes": self._total_disk_writes,
            "pass_num": kwargs.get("pass_num", None),
            "run_id": kwargs.get("run_id", None),
            "value": kwargs.get("value", None),
            "flushed": kwargs.get("flushed", []),
            "heap_state": kwargs.get("heap_state", []),
            "buffers": kwargs.get("buffers", []),
            "block": kwargs.get("block", []),
            "msg": kwargs.get("msg", ""),
        }
        for k, v in kwargs.items():
            if k not in entry:
                entry[k] = v
        self.logs.append(entry)

    def _write_run_to_disk(self, data: List[float], run_id: int, pass_num: int) -> str:
        """Ghi một mảng số xuống file vật lý"""
        filename = os.path.join(self.temp_dir, f"pass{pass_num}_run{run_id}.bin")
        with open(filename, 'wb') as f:
            for num in data:
                f.write(struct.pack(self.format, num))
        self._total_disk_writes += len(data)
        return filename

    def _read_block_from_file(self, f) -> List[float]:
        """Đọc một block từ file đang mở"""
        raw = f.read(self.size_of_double * self.block_size)
        if not raw:
            return []
        
        count = len(raw) // self.size_of_double
        # Unpack binary data thành list float
        vals = [round(x[0], 6) for x in struct.iter_unpack(self.format, raw)]
        self._total_disk_reads += count
        return vals

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

                # Đọc cho đến khi đầy chunk size
                while len(chunk) < self.chunk_size:
                    # Đọc từ file gốc
                    vals = self._read_block_from_file(f)
                    if not vals:
                        break

                    # Log việc đọc buffer (Frontend cần cái này)
                    self._log(
                        "BUFFER_LOAD",
                        pass_num=0,
                        run_id=run_idx,
                        block=list(vals),
                        msg=f"Phase 1 · Read block ({len(vals)} items) for run {run_idx}"
                    )

                    need = self.chunk_size - len(chunk)
                    if len(vals) <= need:
                        chunk.extend(vals)
                    else:
                        chunk.extend(vals[:need])
                        leftover = vals[need:]

                if not chunk:
                    break

                # Sort trong RAM
                chunk.sort()
                
                # Ghi xuống đĩa thật (Real Disk Write)
                run_path = self._write_run_to_disk(chunk, run_idx, 0)
                self.disk_runs.append(run_path)

                self._log(
                    "RUN_CREATED",
                    pass_num=0,
                    run_id=run_idx,
                    values=list(chunk), # Frontend cần giá trị để hiển thị
                    msg=f"Phase 1 · Run {run_idx} created ({len(chunk)} items) and written to disk"
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

    def _merge_group(self, group_paths: List[str], pass_num: int, group_idx: int) -> str:
        # Mở tất cả các file trong nhóm (Real Disk Read)
        files = [open(p, 'rb') for p in group_paths]
        num = len(files)
        
        input_buffers: List[List[float]] = [[] for _ in range(num)]
        min_heap: List[tuple] = []
        
        # Buffer đầu ra trước khi ghi xuống đĩa
        output_buffer: List[float] = []
        
        # Tên file kết quả của nhóm này
        new_run_id = group_idx * self.k_way # ID định danh ảo cho log
        temp_out_path = os.path.join(self.temp_dir, f"pass{pass_num}_group{group_idx}.bin")
        out_f = open(temp_out_path, 'wb')

        # 1. Fill buffers lần đầu
        for i in range(num):
            block = self._read_block_from_file(files[i])
            if block:
                input_buffers[i].extend(block)
                self._log("BUFFER_LOAD", pass_num=pass_num, run_id=new_run_id + i,
                          block=list(block), buffers=[list(b) for b in input_buffers],
                          heap_state=[h[0] for h in min_heap],
                          msg=f"Pass {pass_num} · Load block into Buffer[{i}]")

        # 2. Đẩy vào Heap
        for i in range(num):
            if input_buffers[i]:
                val = input_buffers[i].pop(0)
                heapq.heappush(min_heap, (val, i))
                self._log("HEAP_PUSH", pass_num=pass_num, run_id=new_run_id + i, value=val,
                          buffers=[list(b) for b in input_buffers], heap_state=[h[0] for h in min_heap],
                          msg=f"Pass {pass_num} · Push {val} from Buffer[{i}]")

        # 3. Vòng lặp Merge
        while min_heap:
            val, local_idx = heapq.heappop(min_heap)

            self._log("HEAP_POP", pass_num=pass_num, run_id=new_run_id + local_idx, value=val,
                      buffers=[list(b) for b in input_buffers], heap_state=[h[0] for h in min_heap],
                      msg=f"Pass {pass_num} · Pop {val}")

            output_buffer.append(val)

            # Flush output buffer xuống đĩa thật nếu đầy
            if len(output_buffer) >= self.block_size:
                flushed = list(output_buffer)
                for v in flushed:
                    out_f.write(struct.pack(self.format, v))
                self._total_disk_writes += len(flushed)
                
                self._log("OUTPUT_FLUSH", pass_num=pass_num, run_id=new_run_id + local_idx, flushed=flushed,
                          buffers=[list(b) for b in input_buffers], heap_state=[h[0] for h in min_heap],
                          is_final_flush=False, msg=f"Pass {pass_num} · Flush {len(flushed)} items to disk")
                output_buffer = []

            # Refill input buffer nếu rỗng
            if not input_buffers[local_idx]:
                block = self._read_block_from_file(files[local_idx])
                if block:
                    input_buffers[local_idx].extend(block)
                    self._log("BUFFER_LOAD", pass_num=pass_num, run_id=new_run_id + local_idx,
                              block=list(block), buffers=[list(b) for b in input_buffers],
                              heap_state=[h[0] for h in min_heap],
                              msg=f"Pass {pass_num} · Refill Buffer[{local_idx}]")

            # Push giá trị tiếp theo vào heap
            if input_buffers[local_idx]:
                nxt = input_buffers[local_idx].pop(0)
                heapq.heappush(min_heap, (nxt, local_idx))
                self._log("HEAP_PUSH", pass_num=pass_num, run_id=new_run_id + local_idx, value=nxt,
                          buffers=[list(b) for b in input_buffers], heap_state=[h[0] for h in min_heap],
                          msg=f"Pass {pass_num} · Push {nxt}")

        # Final flush
        if output_buffer:
            flushed = list(output_buffer)
            for v in flushed:
                out_f.write(struct.pack(self.format, v))
            self._total_disk_writes += len(flushed)
            
            self._log("OUTPUT_FLUSH", pass_num=pass_num, run_id=-1, flushed=flushed,
                      buffers=[list(b) for b in input_buffers], heap_state=[], is_final_flush=True,
                      msg=f"Pass {pass_num} · Final flush {len(flushed)} items")

        # Đóng file
        out_f.close()
        for f in files:
            f.close()
            
        return temp_out_path

    def _phase2(self):
        # self.disk_runs bây giờ là List[str] (đường dẫn file)
        current_runs = list(self.disk_runs)
        pass_num = 1

        # Trường hợp chỉ có 1 run (đã sort xong ngay phase 1)
        if len(current_runs) <= 1:
            if current_runs:
                # Copy file tạm ra file output chính thức
                shutil.copy(current_runs[0], self.output_path)
                
            self._log("SORT_COMPLETE", pass_num=0, total_elements=0, # total ko quan trọng ở đây
                      total_disk_reads=self._total_disk_reads, total_disk_writes=self._total_disk_writes,
                      msg="Sort complete (single run)")
            return

        while len(current_runs) > 1:
            reads_before = self._total_disk_reads
            writes_before = self._total_disk_writes

            total_groups = math.ceil(len(current_runs) / self.k_way)
            self._log("PASS_START", pass_num=pass_num, num_runs=len(current_runs), k_way=self.k_way,
                      msg=f"Pass {pass_num} · Merging {len(current_runs)} runs")

            next_runs_paths: List[str] = []

            for g in range(total_groups):
                # Lấy danh sách đường dẫn file cho nhóm này
                group_paths = current_runs[g * self.k_way: (g + 1) * self.k_way]
                
                # Merge và nhận về đường dẫn file kết quả mới
                merged_path = self._merge_group(group_paths, pass_num, g)
                next_runs_paths.append(merged_path)

                # Optional: Xóa các file run cũ để tiết kiệm đĩa
                # for p in group_paths: os.remove(p)

                self._log("PASS_GROUP_DONE", pass_num=pass_num, group_idx=g,
                          msg=f"Pass {pass_num} · Group {g} merged")

            self.pass_stats.append({
                "pass": pass_num,
                "label": f"Pass {pass_num} – Merge",
                "disk_reads": self._total_disk_reads - reads_before,
                "disk_writes": self._total_disk_writes - writes_before,
                "runs_in": len(current_runs),
                "runs_out": len(next_runs_paths)
            })

            current_runs = next_runs_paths
            pass_num += 1

        # Copy file kết quả cuối cùng ra output_path
        if current_runs:
            shutil.move(current_runs[0], self.output_path)
            # Cần tính tổng số phần tử để log
            final_size = os.path.getsize(self.output_path) // self.size_of_double
        else:
            final_size = 0

        self._log("SORT_COMPLETE", pass_num=pass_num - 1, total_elements=final_size,
                  total_passes=pass_num - 1, total_disk_reads=self._total_disk_reads,
                  total_disk_writes=self._total_disk_writes,
                  msg=f"✓ Sort complete · {final_size} items")

    def run_simulation(self) -> Dict[str, Any]:
        self.logs = []
        self.disk_runs = []
        self.pass_stats = []
        self._total_disk_reads = 0
        self._total_disk_writes = 0

        try:
            self._phase1()
            self._phase2()
        finally:
            self.cleanup() # Dọn dẹp file tạm

        summary = {
            "total_steps": len(self.logs),
            "total_runs": 0, # Ko cần thiết cho summary backend
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
