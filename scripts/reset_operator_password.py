"""
Script untuk reset password operator ke 'operator123' (plain text)
"""

import os
from dotenv import load_dotenv
import sqlite3

# Load environment variables
load_dotenv()

# Konfigurasi database SQLite
DB_NAME = os.getenv('DB_NAME', 'bgtk_db.db')
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), DB_NAME)

def reset_operator_password():
    """Reset password operator ke 'operator123'"""
    print("=" * 60)
    print("RESET PASSWORD operator")
    print("=" * 60)
    
    connection = None
    try:
        connection = sqlite3.connect(DB_PATH, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        cursor = connection.cursor()
        
        # Reset password operator (plain text)
        operator_password = 'operator123'
        
        cursor.execute(
            "UPDATE users SET password = ?, role = 'operator' WHERE username = 'operator'",
            (operator_password,)
        )
        connection.commit()
        
        if cursor.rowcount > 0:
            print(f"\n✅ Password operator berhasil di-reset!")
            print(f"   Username: operator")
            print(f"   Password: {operator_password}")
            print("\nSilakan login dengan kredensial di atas.")
        else:
            print(f"\n⚠️  User 'operator' tidak ditemukan!")
            print("   Pastikan user operator sudah dibuat terlebih dahulu.")
        
    except sqlite3.Error as e:
        print(f"\n❌ Error: {e}")
    finally:
        if connection:
            cursor.close()
            connection.close()

if __name__ == '__main__':
    reset_operator_password()

