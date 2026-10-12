# Đối chiếu GUIDE và RUBRIC

- MSSV: `2A202602395`; tên: Đoàn Quang Thắng; repo GitHub Public.
- Dữ liệu: 6/6 file khớp SHA-256 bản chuẩn.
- Pipeline: lần cuối chạy `scripts/lab.sh step all --reset`; xem `evidence/run_summary.json`, `verify.txt`.
- Bronze: 3021/3234 dòng, 26 trip; nạp lại không thêm dòng.
- Silver telemetry: quarantine 53 dòng/drive; offset B 1500 ms đã trừ.
- Bốn thí nghiệm chính thức: `idempotency.json`, `break_gate.json`, `leakage.json`, `threshold.json`.
- Đổi MSSV và khôi phục: `student_id_version.json`.
- DAG từ lake trống: `airflow_clean_reset.json`, `airflow_clean_runs.json`, `airflow_clean_states.json`.
- Sập và chạy lại từ lake trống: `airflow_failure.log`, trạng thái failure/recovery và ảnh tương ứng.
- Dashboard có câu hỏi riêng: `metabase_question.json`, ảnh `metabase_dashboard.png` và `metabase_question.png`.
- Báo cáo: đủ 10 câu, bảng quarantine, lineage, SQL riêng và phần nhìn lại.
- ZIP chỉ bỏ theo dõi Git; vẫn giữ bản trên máy. Không sửa mã pipeline hoặc các file được bảo vệ.
- MSSV có tiền tố chữ nên chọn trip bằng phần số `202602395 % 26 = 21`: `trip-07900`.
- Airflow Clear mặc định gồm Downstream: 1 task khung lỗi và 7 task bị chặn; không chạy lại 25 task khung đã thành công.
