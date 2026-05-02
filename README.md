# headtail

Distributed data processing pipeline berbasis arsitektur **Master-Slave** menggunakan **RPC**, dirancang untuk kebutuhan **ETL (Extract → Transform → Load)**.

Master berperan sebagai **head** — orchestrator yang menerima job dan mendistribusikan task ke slave. Slave berperan sebagai **tail** — worker yang mengeksekusi setiap tahap pipeline dan mengembalikan hasilnya ke master.

## Arsitektur

```
  Job ──▶ Master (head)
              │
       ┌──────┼──────┐
       ▼      ▼      ▼
    Slave   Slave  Slave   (tail)
   Extract  Transform Load
       │      │      │
       └──────┴──────┘
              │
           Results
```

Setiap slave mendaftar ke master saat startup via RPC. Master kemudian mendistribusikan task sesuai stage-nya masing-masing — Extract, Transform, atau Load — dan mengumpulkan hasilnya setelah selesai.

## Alur ETL

1. **Extract** — Slave membaca data dari sumber (database, file, API)
2. **Transform** — Slave membersihkan, memvalidasi, dan mentransformasi data
3. **Load** — Slave menulis hasil ke tujuan akhir (data warehouse, file, stream)

## Teknologi

- **Bahasa**: Go
- **Komunikasi**: `net/rpc` (standard library)
- **Pattern**: Master-Slave Pipeline
