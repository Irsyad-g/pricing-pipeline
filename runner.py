import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import time
import shutil
from pathlib import Path
import traceback


RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

STEPS = [
    "Load data",
    "Extract quota",
    "Process subscription",
    "Build pivots & distributions",
    "Calculate behaviour factor",
    "Build summaries",
    "Export pricing (Google Sheets)",
    "Export to database",
    "Export to Excel",
]


class PipelineGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Global Komunika")
        self.root.geometry("600x620")
        self.root.resizable(False, False)
        self.root.configure(bg="#f5f5f5")

        self.daily_files = []
        self.sub_files   = []

        self._build_ui()

    def _build_ui(self):
        # ── HEADER ──────────────────────────────────────────
        header = tk.Frame(self.root, bg="#1a73e8", pady=14)
        header.pack(fill="x")
        tk.Label(
            header, text="GK Pipeline Runner",
            font=("Segoe UI", 16, "bold"),
            fg="white", bg="#1a73e8"
        ).pack()

        # ── UPLOAD SECTION ───────────────────────────────────
        upload_frame = tk.LabelFrame(
            self.root, text="  Upload File Excel  ",
            font=("Segoe UI", 10, "bold"),
            bg="#f5f5f5", fg="#333", padx=14, pady=10
        )
        upload_frame.pack(fill="x", padx=20, pady=(16, 8))

        # Daily
        tk.Label(upload_frame, text="Daily Usage (.xlsx):",
                 font=("Segoe UI", 9), bg="#f5f5f5").grid(row=0, column=0, sticky="w")
        self.daily_label = tk.Label(
            upload_frame, text="Belum dipilih",
            font=("Segoe UI", 9), fg="#888", bg="#f5f5f5"
        )
        self.daily_label.grid(row=0, column=1, sticky="w", padx=8)
        tk.Button(
            upload_frame, text="Browse",
            command=self._browse_daily,
            bg="#1a73e8", fg="white",
            font=("Segoe UI", 9), relief="flat",
            padx=10, cursor="hand2"
        ).grid(row=0, column=2, padx=4)

        # Subscription
        tk.Label(upload_frame, text="Subscription (.xlsx):",
                 font=("Segoe UI", 9), bg="#f5f5f5").grid(row=1, column=0, sticky="w", pady=(8,0))
        self.sub_label = tk.Label(
            upload_frame, text="Belum dipilih",
            font=("Segoe UI", 9), fg="#888", bg="#f5f5f5"
        )
        self.sub_label.grid(row=1, column=1, sticky="w", padx=8, pady=(8,0))
        tk.Button(
            upload_frame, text="Browse",
            command=self._browse_sub,
            bg="#1a73e8", fg="white",
            font=("Segoe UI", 9), relief="flat",
            padx=10, cursor="hand2"
        ).grid(row=1, column=2, padx=4, pady=(8,0))

        # ── RUN BUTTON ───────────────────────────────────────
        self.run_btn = tk.Button(
            self.root, text="▶  RUN PIPELINE",
            command=self._run,
            bg="#34a853", fg="white",
            font=("Segoe UI", 13, "bold"),
            relief="flat", pady=10,
            cursor="hand2", activebackground="#2d8f46"
        )
        self.run_btn.pack(fill="x", padx=20, pady=8)

        # ── PROGRESS ─────────────────────────────────────────
        prog_frame = tk.LabelFrame(
            self.root, text="  Progress  ",
            font=("Segoe UI", 10, "bold"),
            bg="#f5f5f5", fg="#333", padx=14, pady=10
        )
        prog_frame.pack(fill="x", padx=20, pady=(0, 8))

        self.progress = ttk.Progressbar(
            prog_frame, length=520, mode="determinate",
            maximum=len(STEPS)
        )
        self.progress.pack(fill="x", pady=(0, 6))

        self.progress_label = tk.Label(
            prog_frame, text="Menunggu...",
            font=("Segoe UI", 9), fg="#555", bg="#f5f5f5"
        )
        self.progress_label.pack(anchor="w")

        # ── LOG ──────────────────────────────────────────────
        log_frame = tk.LabelFrame(
            self.root, text="  Log  ",
            font=("Segoe UI", 10, "bold"),
            bg="#f5f5f5", fg="#333", padx=14, pady=10
        )
        log_frame.pack(fill="both", expand=True, padx=20, pady=(0, 16))

        self.log = tk.Text(
            log_frame, height=12,
            font=("Consolas", 9),
            bg="#1e1e1e", fg="#d4d4d4",
            relief="flat", state="disabled"
        )
        self.log.pack(fill="both", expand=True)

        scrollbar = ttk.Scrollbar(log_frame, command=self.log.yview)
        scrollbar.pack(side="right", fill="y")
        self.log["yscrollcommand"] = scrollbar.set

    # ── FILE BROWSER ─────────────────────────────────────────
    def _browse_daily(self):
        files = filedialog.askopenfilenames(
            title="Pilih file Daily Usage",
            filetypes=[("Excel files", "*.xlsx")]
        )
        if files:
            self.daily_files = list(files)
            names = ", ".join(Path(f).name for f in files)
            self.daily_label.config(text=names, fg="#1a73e8")

    def _browse_sub(self):
        files = filedialog.askopenfilenames(
            title="Pilih file Subscription",
            filetypes=[("Excel files", "*.xlsx")]
        )
        if files:
            self.sub_files = list(files)
            names = ", ".join(Path(f).name for f in files)
            self.sub_label.config(text=names, fg="#1a73e8")

    # ── LOGGING ──────────────────────────────────────────────
    def _log(self, msg, color="#d4d4d4"):
        self.log.config(state="normal")
        self.log.insert("end", msg + "\n")
        self.log.tag_add("last", "end-2l", "end-1l")
        self.log.tag_config("last", foreground=color)
        self.log.see("end")
        self.log.config(state="disabled")

    def _set_progress(self, step, label):
        self.progress["value"] = step
        pct = int(step / len(STEPS) * 100)
        self.progress_label.config(text=f"Step {step}/{len(STEPS)} — {label} ({pct}%)")

    # ── RUN ──────────────────────────────────────────────────
    def _run(self):
        if not self.daily_files and not self.sub_files:
            messagebox.showwarning("File belum dipilih", "Pilih minimal 1 file Excel dulu.")
            return

        self.run_btn.config(state="disabled", bg="#aaa", text="⏳  Running...")
        self.log.config(state="normal")
        self.log.delete("1.0", "end")
        self.log.config(state="disabled")
        self.progress["value"] = 0

        thread = threading.Thread(target=self._run_pipeline, daemon=True)
        thread.start()

    def _run_pipeline(self):
        try:
            # ── COPY FILE KE data/raw/ ───────────────────────
            self._log("📁 Menyalin file ke data/raw/ ...")
            all_files = self.daily_files + self.sub_files
            for f in all_files:
                dest = RAW_DIR / Path(f).name
                shutil.copy2(f, dest)
                self._log(f"  ✅ {Path(f).name}", "#6fcf97")

            # ── IMPORT PIPELINE ──────────────────────────────
            from config import RAW_DATA, OUTPUT_FILE
            from loaders import load_daily_usage, load_subscription
            from rules.quota_rules import extract_quota
            from processors import process_subscription, build_country_distribution, split_country_dist_by_region
            from pivots import build_country_usage_pivot
            from summaries.month_summary import build_month_summary
            from summaries.summary import build_summary, build_base_factor
            from processors.behaviour_factor import calculate_behaviour_factor
            from exporters.excel_exporter import export_all
            from exporters.google_sheets_exporter import export_pricing
            from exporters.db_exporter import export_to_db

            def step(n, label, fn, *args, **kwargs):
                self._set_progress(n, label)
                self._log(f"\n⏳ {label}...")
                t = time.perf_counter()
                result = fn(*args, **kwargs)
                elapsed = time.perf_counter() - t
                self._log(f"  ✅ {label} ({elapsed:.1f}s)", "#6fcf97")
                return result

            # ── STEPS ────────────────────────────────────────
            sub_files  = list(RAW_DATA.glob("SUBSCRIPTION_*.xlsx"))
            daily_files = (
                list(RAW_DATA.glob("DAILY_USAGE_*.xlsx")) +
                list(RAW_DATA.glob("BSN-*.xlsx"))
            )

            daily = step(1, "Load data", lambda: (
                load_daily_usage(daily_files),
                load_subscription(sub_files)
            ))
            daily, sub = daily

            sub["TOTAL_QUOTA_MB"] = step(2, "Extract quota",
                lambda: sub.assign(TOTAL_QUOTA_MB=sub.apply(
                    lambda x: extract_quota(x["PACKAGE"], x["DAYS"]), axis=1
                ))["TOTAL_QUOTA_MB"]
            )

            final, country_df = step(3, "Process subscription",
                process_subscription, sub, daily)

            step(4, "Build pivots & distributions",
                lambda: (
                    build_country_usage_pivot(country_df),
                    build_country_distribution(country_df)
                )
            )
            country_dist_wide = build_country_distribution(country_df)

            bf_table, bf_full = step(5, "Calculate behaviour factor",
                calculate_behaviour_factor, final)

            summary, month_summary, base = step(6, "Build summaries",
                lambda: (build_summary(final), build_month_summary(final), build_base_factor(final))
            )

            pricing_df = step(7, "Export pricing (Google Sheets)",
                export_pricing, bf_full, country_df)

            step(8, "Export to database",
                export_to_db,
                daily=daily, sub=sub, final=final,
                bf_full=bf_full, pricing_df=pricing_df,
                country_df=country_df,
                daily_files=daily_files, sub_files=sub_files
            )

            step(9, "Export to Excel",
                export_all, OUTPUT_FILE,
                ALL_DATA=final,
                SUMMARY=summary,
                MONTH_SUMMARY=month_summary,
                Behaviour_Factor=bf_table,
                Behaviour_Full=bf_full,
                Base_Factor=base
            )

            self._set_progress(len(STEPS), "Selesai")
            self._log("\n🎉 PIPELINE SELESAI!", "#6fcf97")
            self.root.after(0, lambda: messagebox.showinfo(
                "Selesai", "Pipeline berhasil dijalankan!"
            ))

        except Exception as e:
            err = traceback.format_exc()
            self._log(f"\n❌ ERROR:\n{err}", "#eb5757")
            self.root.after(0, lambda: messagebox.showerror(
                "Error", f"Pipeline gagal:\n{str(e)}"
            ))

        finally:
            self.root.after(0, lambda: self.run_btn.config(
                state="normal", bg="#34a853", text="▶  RUN PIPELINE"
            ))


if __name__ == "__main__":
    root = tk.Tk()
    app = PipelineGUI(root)
    root.mainloop()