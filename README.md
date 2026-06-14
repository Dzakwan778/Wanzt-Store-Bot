# 🚀 Wanzt Store Bot

Sistem Manajemen Toko Digital berbasis WhatsApp yang dilengkapi dashboard modern, manajemen produk, transaksi, broadcast otomatis, sistem admin multi-role, dan integrasi WhatsApp menggunakan Baileys.

---

## ✨ Fitur Utama

### 🤖 WhatsApp Bot

- Login QR Code
- Login Pairing Code
- Auto Response Produk
- Menu Otomatis
- Katalog Produk
- Template Pesan Kustom
- Command Custom
- Sistem Owner & Admin
- Whitelist Group
- Logging Pesan

### 🛍️ Manajemen Produk

- Tambah Produk
- Edit Produk
- Hapus Produk
- Kategori Produk
- Gambar Produk
- Varian Produk
- Harga Modal & Harga Jual
- Tracking Pencarian Produk

### 💳 Manajemen Transaksi

- Pencatatan Transaksi
- Status Pending
- Status Success
- Status Failed
- Riwayat Transaksi
- Perhitungan Harga Jual
- Perhitungan Harga Modal

### 📢 Broadcast Management

- Broadcast Manual
- Broadcast Terjadwal
- Broadcast Gambar
- Broadcast Video
- Target Berdasarkan Kategori
- Excluded Number List
- Delay Antar Pesan
- Log Pengiriman

### 👥 Admin Management

- Login Dashboard
- Multi Admin
- Owner Role
- Admin Role
- Permission Management
- Session Authentication
- Activity Log

### 📊 Dashboard Monitoring

- Status Bot
- Status WhatsApp
- Statistik Produk
- Statistik Transaksi
- Statistik Broadcast
- Monitoring Aktivitas

### 🔐 Security

- Session Token Authentication
- Permission Based Access
- Admin Activity Logging
- Multi-Level Access Control

---

## 🏗️ Teknologi

### Frontend

- React 19
- TypeScript
- Vite
- Tailwind CSS
- Motion
- Lucide React

### Backend

- Node.js
- Express

### WhatsApp Gateway

- Baileys

### Cloud & Database

- Firebase (Blueprint tersedia)
- Firestore Rules
- JSON Database Local

---

## 📂 Struktur Project

```text
Wanzt-Store-Bot
│
├── src/
│   ├── App.tsx
│   ├── firebase-db.ts
│   ├── types.ts
│   └── ...
│
├── server.ts
├── package.json
├── firestore.rules
├── firebase-blueprint.json
├── vite.config.ts
├── tsconfig.json
├── .env.example
└── README.md
```

---

## 📦 Instalasi

### Clone Repository

```bash
git clone https://github.com/Dzakwan778/Wanzt-Store-Bot.git
cd Wanzt-Store-Bot
```

### Install Dependency

```bash
npm install
```

---

## ⚙️ Konfigurasi Environment

Salin file:

```bash
.env.example
```

menjadi:

```bash
.env
```

Lalu sesuaikan:

```env
GEMINI_API_KEY=YOUR_API_KEY
APP_URL=YOUR_APP_URL
```

---

## ▶️ Menjalankan Project

### Development Mode

```bash
npm run dev
```

Server akan berjalan pada:

```text
http://localhost:3000
```

---

### Production Build

```bash
npm run build
```

Menjalankan hasil build:

```bash
npm start
```

---

## 📱 Koneksi WhatsApp

Sistem mendukung:

### QR Code Login

1. Jalankan server
2. Buka Dashboard
3. Scan QR yang muncul
4. Tunggu hingga status Connected

### Pairing Code

1. Masukkan nomor WhatsApp
2. Generate Pairing Code
3. Masukkan kode pada WhatsApp
4. Tunggu koneksi berhasil

---

## 🛒 Sistem Produk

Setiap produk mendukung:

- Nama Produk
- Kategori
- Deskripsi
- Gambar
- Varian Produk
- Harga Modal
- Harga Jual
- Stok

Contoh:

```text
Netflix Premium

├── 1 Bulan Shared
├── 1 Bulan Private
├── 3 Bulan Private
└── 12 Bulan Private
```

---

## 📋 Sistem Transaksi

Status transaksi:

| Status | Keterangan |
|----------|------------|
| Pending | Menunggu proses |
| Success | Berhasil diselesaikan |
| Failed | Gagal diproses |

Setiap transaksi menyimpan:

- ID Pesanan
- Nama Produk
- Nomor Pembeli
- Metode Pembayaran
- Harga Modal
- Harga Jual
- Timestamp

---

## 📢 Broadcast Terjadwal

Fitur broadcast mendukung:

- Text Message
- Image Message
- Video Message
- Scheduled Delivery
- Target Filtering
- Delivery Logs

---

## 👨‍💻 Sistem Role

### OWNER

Akses penuh ke:

- Produk
- Broadcast
- Transaksi
- Pengaturan
- Admin Management
- Aktivitas Sistem

### ADMIN

Akses berdasarkan permission yang diberikan Owner.

---

## 🔧 Script NPM

```bash
npm run dev
```

Menjalankan development server.

```bash
npm run build
```

Build frontend dan backend.

```bash
npm start
```

Menjalankan production server.

```bash
npm run lint
```

TypeScript validation.

```bash
npm run clean
```

Membersihkan folder build.

---

## 💾 Database

Project menggunakan kombinasi:

### Local Database

```text
database.json
db_products.json
db_categories.json
db_transactions.json
db_commands.json
db_settings.json
db_scheduled_broadcasts.json
```

### Cloud Database

- Firebase
- Firestore

(Struktur sudah disiapkan dan dapat diaktifkan sesuai kebutuhan.)

---

## 📌 Status Project

🟢 Aktif Dikembangkan

Roadmap:

- Firebase Full Sync
- Multi Device Support
- Payment Gateway Integration
- Advanced Analytics
- Customer CRM Module

---

## 🤝 Kontribusi

Pull Request dan Issue sangat diterima.

1. Fork repository
2. Buat branch baru
3. Commit perubahan
4. Push branch
5. Buat Pull Request

---

## 👨‍💻 Developer

**Dzakwan778**

GitHub:
https://github.com/Dzakwan778

---

## 📄 License

Project ini dibuat untuk kebutuhan operasional Wanzt Store dan pembelajaran pengembangan sistem manajemen WhatsApp berbasis dashboard.

Silakan gunakan dan modifikasi sesuai kebutuhan.
