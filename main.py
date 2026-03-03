import os
from fastapi import FastAPI, UploadFile, File, Form
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from core_sort import ExternalSortSimulator

app = FastAPI()

# Cho phép CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Tự động xác định đường dẫn thư mục hiện tại
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FRONTEND_DIR = os.path.join(BASE_DIR, "frontend")
TEMP_DIR = os.path.join(BASE_DIR, "temp_files")

os.makedirs(TEMP_DIR, exist_ok=True)

# Đường dẫn file làm việc
INPUT_PATH = os.path.join(TEMP_DIR, "input.bin")
INPUT_DEMO_PATH = os.path.join(TEMP_DIR, "input_demo.bin")
OUTPUT_PATH = os.path.join(TEMP_DIR, "output_sorted.bin")
OUTPUT_DEMO_PATH = os.path.join(TEMP_DIR, "output_demo.bin")

@app.post("/sort")
async def sort_full(file: UploadFile = File(...), chunk_size: int = Form(64), block_size: int = Form(16), k_way: int = Form(4)):
    try:
        content = await file.read()
        with open(INPUT_PATH, "wb") as f: f.write(content)
        sim = ExternalSortSimulator(INPUT_PATH, OUTPUT_PATH, chunk_size, block_size, k_way)
        result = sim.run_simulation()
        summary = result.get("summary", {})
        return {
            "total_elements": len(content) // 8,
            "original_size_kb": round(len(content) / 1024, 2),
            "total_disk_reads": summary.get("total_disk_reads", 0),
            "total_disk_writes": summary.get("total_disk_writes", 0),
            "total_passes": len(result.get("pass_stats", [])) - 1
        }
    except Exception as e: return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/simulate")
async def simulate(file: UploadFile = File(...), chunk_size: int = Form(15), block_size: int = Form(2), k_way: int = Form(2)):
    try:
        content = await file.read()
        total = len(content) // 8
        demo_count = min(total, 500)
        with open(INPUT_DEMO_PATH, "wb") as f: f.write(content[:demo_count * 8])
        sim = ExternalSortSimulator(INPUT_DEMO_PATH, OUTPUT_DEMO_PATH, chunk_size, block_size, k_way)
        result = sim.run_simulation()
        result.update({"truncated": total > 500, "original_count": total, "demo_count": demo_count})
        return result
    except Exception as e: return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/download")
def download_full(): return FileResponse(OUTPUT_PATH, filename="sorted.bin")

@app.get("/download/demo")
def download_demo(): return FileResponse(OUTPUT_DEMO_PATH, filename="sorted_demo.bin")

# --- PHỤC VỤ FRONTEND ---
# 1. Mount folder frontend để load script.js qua /static/script.js
app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

# 2. Trả về file index.html khi vào địa chỉ gốc /
@app.get("/")
async def serve_index():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))