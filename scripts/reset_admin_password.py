"""
Script untuk reset password admin ke 'admin123' (plain text)
"""

import os
from dotenv import load_dotenv
import sqlite3

# Load environment variables
load_dotenv()

# Konfigurasi database SQLite
DB_NAME = os.getenv('DB_NAME', 'bgtk_db.db')
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), DB_NAME)

def reset_admin_password():
    """Reset password admin ke 'admin123'"""
    print("=" * 60)
    print("RESET PASSWORD ADMIN")
    print("=" * 60)
    
    connection = None
    try:
        connection = sqlite3.connect(DB_PATH, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()
        
        # Reset password admin (plain text)
        admin_password = 'admin123'
        
        cursor.execute(
            "UPDATE users SET password = ?, role = 'admin' WHERE username = 'admin'",
            (admin_password,)
        )
        connection.commit()
        
        if cursor.rowcount > 0:
            print(f"\n✅ Password admin berhasil di-reset!")
            print(f"   Username: admin")
            print(f"   Password: {admin_password}")
            print("\nSilakan login dengan kredensial di atas.")
        else:
            print(f"\n⚠️  User 'admin' tidak ditemukan!")
            print("   Pastikan user admin sudah dibuat terlebih dahulu.")
        
    except sqlite3.Error as e:
        print(f"\n❌ Error: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()

if __name__ == '__main__':
    reset_admin_password()

