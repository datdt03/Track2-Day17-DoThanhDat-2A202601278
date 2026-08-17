# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Đỗ Thành Đạt  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

## 0 · Kết quả `make verify`

```text
run 1/3 … 28.0s
run 2/3 … 26.8s
run 3/3 … 27.6s

BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
gold_training_set     ✓ ok              12,480      12,480   ✓
gold_feature_daily    ✓ ok               9,100       9,100   ✓
gold_doc_chunks       ✓ ok              31,200      31,200   ✓
quarantine_tickets    ✓ ok                 312         312   ✓

CHECKSUM từng lượt
gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

dbt test                                    ✓ 11/11 pass
silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
dashboard rows scanned                      ✓ 5,000,000 → 137,424 (36.4×, cần ≥ 10×)
  số file parquet                           ✓ 5,000 → 14
  kết quả truy vấn không đổi                ✓
DAG: catchup / max_active_runs              ✓ False / 1

TỔNG KẾT
──────────────────────────────────────────────────────────────────────────
✓  1 · gold_training_set idempotent & đúng số hàng
✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
✓  3 · contract + quarantine + dbt test
✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
──────────────────────────────────────────────────────────────────────────
4/4 tiêu chí đạt
```

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Bảng tăng từ 12.480 lên 25.615 rồi 38.750 hàng khi chạy lại; mọi ticket đều bị lặp. |
| **Nguyên nhân** | Model incremental không có `unique_key` và strategy nên dbt append. Retry xử lý lại cùng dữ liệu và chèn thêm thay vì cập nhật. Vì grain là entity và CDC chứa update ở nhiều ngày, xóa theo partition ngày cũng không bảo đảm một hàng mỗi ticket. `catchup=True` và concurrent run làm tăng khả năng kích hoạt lỗi nhưng không phải root cause. |
| **Cách khắc phục** | Dùng `unique_key='ticket_id'`, `incremental_strategy='merge'`; đặt DAG `catchup=False`, `max_active_runs=1`. |
| **Bằng chứng** | 12.480 hàng, không lặp; checksum ba lượt đều `8dd7c98653`. |

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Bảng ổn định nhưng chỉ có 8.645 thay vì 9.100 cặp `(event_date, customer_id)`; thiếu ở các ngày quá khứ. |
| **P99 độ trễ đo được** | **2,7258 ngày**; P50 = 0,1281, P95 = 1,8137, max = 2,9447 ngày; 5,05% event trễ hơn một ngày. |
| **Lookback đã chọn** | **3 ngày**, làm tròn lên từ P99 và cũng bao phủ max quan sát của bộ dữ liệu lab. |
| **Nguyên nhân** | `event_date > max(event_date)` loại vĩnh viễn event cũ tới muộn. Ví dụ event 12/08 tới ngày 15/08 không lọt qua vì target đã có ngày mới hơn. Khi mở window, append cũng sẽ tạo bản sao cho cùng grain. |
| **Cách khắc phục** | Lùi incremental window 3 ngày và merge bằng khóa ghép `['event_date', 'customer_id']`. |
| **Bằng chứng** | 9.100 hàng; checksum ba lượt đều `3db448685c`. |

P99 là ngưỡng phục vụ định lượng, tránh để một outlier buộc mọi lượt chạy quét quá nhiều lịch sử. `max` bao phủ mẫu đã thấy nhưng nhạy với ngoại lệ và tăng chi phí compute lâu dài. Trong lab, làm tròn P99 lên 3 ngày cũng bao phủ max; production nên giám sát dữ liệu vượt window và backfill riêng theo SLA.

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Pipeline và 9 test cũ đều pass nhưng 6.606 giá trị priority sai/NULL; classifier suy giảm sau khi backend đổi biểu diễn. |
| **Nguyên nhân** | `try_cast` biến nhãn hợp lệ thành NULL nhưng lại chấp nhận số ngoài miền. Contract tắt và thiếu domain test. Nếu lọc sau `row_number`, ticket có update lỗi mới nhất sẽ biến mất thay vì quay về trạng thái hợp lệ trước đó. |
| **Ba nhóm giá trị** | `'1'..'4'` → giữ; `urgent/high/medium/low` → 1/2/3/4; `P1`, `unknown`, `0`, `5`, `-1`, rỗng, NULL → quarantine. |
| **Cách khắc phục** | Dùng chung macro `CASE`; lọc lỗi trước khi xếp hạng CDC; quarantine row macro trả NULL; bật contract; thêm `not_null` và `accepted_values`. |
| **Bằng chứng** | Quarantine 312 hàng, Silver vẫn đủ 12.480 ticket, priority sạch, `dbt test` 11/11 pass. |

Bronze nên giữ nguyên để bảo toàn dữ liệu nguồn và khả năng replay/audit; contract nghiệp vụ được thực thi ở Silver. Row lỗi được tách vào quarantine để người trực xử lý, không làm hơn 130.000 event và 31.200 chunk hợp lệ ngừng phục vụ.

## 4 · Bài mở rộng A và B

| | |
|---|---|
| **Nguyên nhân** | A: 5.000 file nhỏ không partition và predicate `strftime` buộc quét toàn bộ. B: commit trước ghi tạo at-most-once và mất batch khi crash; ghi trước commit có replay nên cần phép ghi idempotent. |
| **Cách khắc phục** | A: compact theo `event_date`, sort ngày/customer/time, dùng predicate sargable. B: ghi trước, commit sau, primary key `event_id`, upsert `DO UPDATE` trong transaction theo batch. `DO UPDATE` nhận payload mới khi replay, còn `DO NOTHING` có thể giữ payload cũ. |
| **Bằng chứng** | A: scanned 5.000.000 → 137.424 (**36,4×**), file 5.000 → 14, hash giữ `4379e4c5d9f3`. B: crash/restart vẫn 20.000 hàng/20.000 event duy nhất. |

## 5 · Tổng kết

| Nhiệm vụ | Kiểm tra trước tiên |
|---|---|
| 1 | Grain, natural key, materialization strategy và hành vi khi retry. |
| 2 | Event time so với ingestion time, lateness và incremental window. |
| 3 | Contract kiểu, domain test, quarantine và thứ tự filter/dedup. |
