"""
Script untuk test koneksi database SQLite
Jalankan script ini untuk memeriksa apakah koneksi database berfungsi
"""

import os
from dotenv import load_dotenv
import sqlite3

# Load environment variables
load_dotenv()

# Konfigurasi database SQLite
DB_NAME = os.getenv('DB_NAME', 'bgtk_db.db')
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), DB_NAME)

print("=" * 50)
print("TEST KONEKSI DATABASE SQLITE")
print("=" * 50)
print(f"Database: {DB_NAME}")
print(f"Path: {DB_PATH}")
print("=" * 50)

# Test 1: Koneksi ke database
print("\n[TEST 1] Mencoba koneksi ke database SQLite...")
try:
    connection = sqlite3.connect(DB_PATH, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.execute('PRAGMA foreign_keys = ON')
    print("✅ Koneksi ke database SQLite BERHASIL!")
    connection.close()
except sqlite3.Error as e:
    print(f"❌ Koneksi ke database SQLite GAGAL!")
    print(f"   Error: {e}")
    print("\n💡 SOLUSI:")
    print("   1. Pastikan folder aplikasi memiliki permission write")
    print("   2. Cek apakah path database benar")
    exit(1)

# Test 2: Membuat database jika belum ada
print("\n[TEST 2] Membuat database jika belum ada...")
try:
    connection = sqlite3.connect(DB_PATH, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.execute('PRAGMA foreign_keys = ON')
    connection.commit()
    print(f"✅ Database '{DB_NAME}' siap!")
    connection.close()
except sqlite3.Error as e:
    print(f"❌ Gagal membuat database!")
    print(f"   Error: {e}")
    exit(1)

# Test 3: Koneksi dengan database
print("\n[TEST 3] Mencoba koneksi ke database...")
try:
    connection = sqlite3.connect(DB_PATH, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.execute('PRAGMA foreign_keys = ON')
    print(f"✅ Koneksi ke database '{DB_NAME}' BERHASIL!")
    connection.close()
except sqlite3.Error as e:
    print(f"❌ Koneksi ke database GAGAL!")
    print(f"   Error: {e}")
    exit(1)

# Test 4: Membuat tabel users
print("\n[TEST 4] Membuat tabel 'users' jika belum ada...")
try:
    connection = sqlite3.connect(DB_PATH, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.execute('PRAGMA foreign_keys = ON')
    cursor = connection.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username VARCHAR(50) UNIQUE NOT NULL,
        password VARCHAR(255) NOT NULL,
        role TEXT DEFAULT 'user',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    cursor.execute(create_table_query)
    connection.commit()
    print("✅ Tabel 'users' siap!")
    cursor.close()
    connection.close()
except sqlite3.Error as e:
    print(f"❌ Gagal membuat tabel!")
    print(f"   Error: {e}")
    exit(1)

print("\n" + "=" * 50)
print("🎉 SEMUA TEST BERHASIL!")
print("=" * 50)
print("Database siap digunakan. Anda bisa menjalankan aplikasi dengan:")
print("  python app.py")

