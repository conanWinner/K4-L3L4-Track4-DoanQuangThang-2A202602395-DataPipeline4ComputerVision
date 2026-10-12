# REPORT — Lab Day 9

> Số liệu từ pipeline chạy với MSSV `2A202602395`. Bằng chứng và ảnh chụp được lưu trong `report/`. Các kết quả đều đối chiếu với file bằng chứng; kiểm tra bài nộp và Git được thực hiện sau lần chạy cuối.

| | |
|---|---|
| Họ và tên | `Đoàn Quang Thắng` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602395` |
| GitHub | `conanWinner` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-07900` |

## Kết quả cuối

- `dataset_version` của bạn: `3a7c6d3ebd18`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: 676 / 169 / 174
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, `airflow_failure.png`, `airflow_recovered.png` và `metabase_question.png`.

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Nguồn A dùng `ts_utc` ISO UTC, B dùng `timestamp_ms` epoch mili giây; A dùng `speed_mps`, B dùng `spd_kmh`; A có `heading_deg/lat/lon/alt_m`, B có `hdg/latitude/longitude/altitude`; A dùng `acc_x/acc_y`, B dùng `ax/ay`; A dùng gas/brake trong thang 0–1, B dùng throttle_pct/brake_pct trong thang 0–100. B còn lệch đồng hồ +1.500 ms. Các cột nguồn CSV giữ dạng chữ; cột kỹ thuật `_row_no` là số nguyên và `_ingested_at` là timestamp. Giữ riêng bảng và dữ liệu nguồn dạng chữ giúp bảo toàn bản gốc, kể cả ô trống/lỗi, để kiểm tra và áp dụng lại luật chuẩn hoá. A có 3.021 dòng, B 3.234; nạp lại báo already present, lịch sử bảng A vẫn chỉ có commit phiên bản 0.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Staging đổi tên cột B, chia tốc độ cho 3,6 và bàn đạp cho 100; timestamp epoch được trừ offset 1.500 ms rồi chuyển UTC. Telemetry B bắt đầu `2025-10-08 14:30:00.009+00:00`, lệch video chỉ 9 ms. A còn 2.968 dòng sạch, B 3.181; mỗi drive cách ly 20 duplicate, 30 speed_missing, 3 speed_out_of_range (53 dòng). Tổng sạch 6.149 + rejects 106 = 6.255 nhận vào. Rejects giữ dòng lỗi và lý do để điều tra hoặc xử lý lại, tránh mất dấu dữ liệu.

| Drive | duplicate | speed_missing | speed_out_of_range | Tổng rejects | Dòng sạch |
|---|---:|---:|---:|---:|---:|
| drive_a | 20 | 30 | 3 | 53 | 2968 |
| drive_b | 20 | 30 | 3 | 53 | 3181 |


### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` dùng severity warn: cảm biến mất tín hiệu cần được báo nhưng các phần dữ liệu còn tốt vẫn dùng được. `in_range` mặc định error vì tốc độ ngoài 0–70 m/s có thể làm sai dữ liệu huấn luyện. Thí nghiệm break-gate nới luật lên 1.000 m/s: test `in_range_silver_telemetry_speed_mps__70__0` bắt 6 dòng, kết quả PASS=5 WARN=1 ERROR=1 SKIP=0. Khôi phục luật gốc đạt PASS=6 WARN=1 ERROR=0 SKIP=0. Cổng chất lượng kiểm tra độc lập với luật lọc.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Bảng silver.frames có 1.246 dòng trước và sau hai lần chạy lại trip-07900; dấu vân tay đều `6c7480dfde7e`. Trip này có 30 khung. Thí nghiệm chính thức ghi append hai lần vào bảng Delta thử cho 60 dòng (2×), rồi dọn đúng bảng thử. Delta overwrite theo predicate trip_id tạo commit giao dịch thay thế đúng trip; nếu ghi lỗi trước commit thì snapshot cũ vẫn đọc được, tránh khoảng trống của cách xoá rồi ghi lại. Lưu ý thư mục partition vật lý là drive_id; predicate thay thế dữ liệu của một trip.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

A có 6 khung đen, B có 4; A có 4 khung tối được gắn cờ, B có 61. Đen là độ sáng trung bình <12; tối là từ 12 đến <40. Pipeline loại khung đen nhưng không loại chỉ vì tối; khung tối vẫn có thể bị loại vì mờ, trùng hoặc mất đồng bộ. Cách này phù hợp để giữ cảnh đường hầm/bóng râm có nội dung. Ngưỡng dHash 0/2/6 cho A lần lượt 61/149/363 khung trùng và B 8/53/244. Chọn 2 như pipeline để cân bằng giảm dư thừa và giữ thay đổi nhỏ. Xe chậm hoặc camera đứng hình tạo nhiều khung gần nhau; xe nhanh có thay đổi lớn, nên ngưỡng cần đánh giá theo chuyển động và cảnh.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Trong 1.019 khung được giữ thuộc 26 trip, chia theo khung làm cả 26 trip xuất hiện ở nhiều split, cả 26 có khung ở train và test. Chia theo trip của Gold có 0 trip bị chia cắt. Khung gần nhau chia sẻ cảnh và trạng thái xe; mô hình được kiểm tra trên hình gần giống dữ liệu đã học nên điểm có thể cao giả tạo. Thí nghiệm đo rò rỉ, chưa huấn luyện mô hình để đo mức tăng điểm.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Phiên bản là 12 ký tự đầu SHA-256 của danh sách frame_id|split đã sắp xếp, nhận diện cùng tập mẫu/split bất kể giờ chạy. Nó không băm byte ảnh hay mọi cột telemetry; cần lineage và SHA-256 video để kiểm tra nguồn gốc đầy đủ. Hai lần export đều ra `3a7c6d3ebd18`, với 676 train, 169 val, 174 test (18/4/4 trip). MSSV `2A202602395` tham gia thứ tự chia trip theo drive nên MSSV khác có thể tạo phân bố và version khác. Thử MSSV `2A202602396` thực tế tạo version `77e2743d15f7` và split {'train': 708, 'test': 165, 'val': 146}; khôi phục `2A202602395` trả về version gốc. Đã dùng biến môi trường riêng cho lệnh thử, không thay MSSV lưu trong .env; bằng chứng: student_id_version.json.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Manifest dataset 3a7c6d3ebd18 ghi bronze.raw_telemetry_a=0, bronze.raw_telemetry_b=0, bronze.trips_raw=0, bronze.video_files=1; silver.frames=131, silver.telemetry=5, silver.frames_flagged=4, silver.frame_telemetry=4 tại thời điểm lưu manifest. Video A có SHA-256 `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`, B `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`. Cần giữ snapshot Delta tương ứng, video gốc, MSSV, mã/luật và phiên bản môi trường để tái tạo đúng sau 3 tháng. Xem report/evidence/manifest.json.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Trip bài sập là `trip-07900`: phần số 202602395 modulo 26 bằng 21 trong danh sách sắp xếp. Run `guide_fail_clean_pipeline_20261012` có đúng 1 task đỏ `silver_frames[21]`, 7 task upstream_failed và 29 task success. Chọn Clear ở task lỗi với Downstream: 8 task được đưa về trạng thái chờ, chỉ 1/26 task khung hình chạy lại; 25 task khung đã thành công được giữ nguyên. Sau phục hồi, cả 37 task success, dataset vẫn `3a7c6d3ebd18`; lần chạy toàn bộ sau phục hồi vẫn đạt 27/27, silver.frames vẫn 1.246 dòng và Gold vẫn 1.019 dòng, không nhân đôi dữ liệu. Bằng chứng: airflow_failure.png, airflow_recovered.png và JSON trạng thái/log. Đã trigger day09_reset trước cả DAG bình thường và bài sập; các run reset đều success. DAG bình thường từ lake trống có 37/37 task success, được lưu ở airflow_clean_states.json.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Ba nút thắt dự kiến: (1) FFmpeg decode và tải lại video mỗi trip gây tốn CPU/network; dùng worker phân tán, cache video và lập lịch theo drive. (2) DuckDB một file/một writer và Airflow LocalExecutor hạn chế song song; chuyển biến đổi lớn sang engine phân tán và executor có hàng đợi. (3) sao chép hàng loạt JPG vào mỗi dataset gây nhiều S3 request và dung lượng; dùng manifest tham chiếu ảnh bất biến/shard, object storage đủ băng thông. Đây là dự đoán kiến trúc cho 500 xe/ngày, chưa có benchmark ở quy mô đó.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

```sql
select drive_id, trip_id, frames, frames_kept, frames - frames_kept as frames_dropped, round(100.0 * (frames - frames_kept) / nullif(frames, 0), 2) as dropped_pct from marts.mart_trip_summary order by dropped_pct desc, trip_id;
```

Đã thêm vào dashboard http://localhost:3000/dashboard/2 (card 46). Trip-04622 của drive A có tỷ lệ loại cao nhất: 34/60 khung (56,67%); trip-03904 đứng sau với 11/22 (50%). Tỷ lệ là số khung bị loại duy nhất, không cộng các cờ chồng lấp.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Ghi nhận từ bài chạy: lỗi quyền thư mục bind mount ngăn DuckDB tạo file dù container healthy; kiểm tra health chưa đủ để kết luận pipeline chạy được. Điểm đáng chú ý là khung tối vẫn có thể dùng, và chia theo khung gây rò rỉ ở cả 26 trip. Chưa kiểm chứng tác động tới điểm mô hình. Pipeline chưa có kiểm tra GPS nhảy bất thường hay tốc độ không phù hợp với quãng đường GPS; đây là hướng bổ sung cần dữ liệu và ngưỡng thực tế.
