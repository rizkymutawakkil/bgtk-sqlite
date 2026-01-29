/**
 * Reset Password Handler
 * File khusus untuk menangani reset password operator
 */

/**
 * Fungsi untuk menangani reset password operator
 * @param {number} operatorId - ID operator yang akan di-reset password-nya
 * @param {string} operatorName - Nama operator
 */
function handleResetPassword(operatorId, operatorName) {
    if (!operatorId || !operatorName) {
        console.error('Missing required parameters:', {operatorId, operatorName});
        Swal.fire({
            title: 'Error!',
            text: 'Data operator tidak valid!',
            icon: 'error',
            confirmButtonText: 'OK'
        });
        return;
    }

    // Tampilkan konfirmasi
    Swal.fire({
        title: 'Konfirmasi Reset Password',
        html: `Apakah Anda yakin ingin mereset password untuk operator <strong>${escapeHtml(operatorName)}</strong>?<br><br><div style="background: #fef3c7; padding: 12px; border-radius: 6px; margin: 10px 0;"><strong>Password akan direset menjadi:</strong><br><code style="background: #fff; padding: 4px 8px; border-radius: 4px; font-size: 14px;">operator123</code></div><small style="color: #6b7280;">Operator perlu login ulang dengan password baru setelah reset.</small>`,
        icon: 'warning',
        showCancelButton: true,
        confirmButtonColor: '#f59e0b',
        cancelButtonColor: '#6b7280',
        confirmButtonText: 'Ya, Reset Password',
        cancelButtonText: 'Batal',
        reverseButtons: true,
        allowOutsideClick: false,
        allowEscapeKey: true
    }).then((result) => {
        if (result.isConfirmed) {
            // Submit form untuk reset password
            submitResetPassword(operatorId);
        }
    });
}

/**
 * Submit form untuk reset password
 * @param {number} operatorId - ID operator
 */
function submitResetPassword(operatorId) {
    // Buat form untuk submit POST request
    const form = document.createElement('form');
    form.method = 'POST';
    form.action = `/admin/reset-password-operator/${operatorId}`;
    
    // Tambahkan CSRF token
    const csrfInput = document.createElement('input');
    csrfInput.type = 'hidden';
    csrfInput.name = 'csrf_token';
    
    // Ambil CSRF token dari meta tag atau input hidden yang ada di halaman
    const csrfToken = getCSRFToken();
    if (!csrfToken) {
        Swal.fire({
            title: 'Error!',
            text: 'CSRF token tidak ditemukan!',
            icon: 'error',
            confirmButtonText: 'OK'
        });
        return;
    }
    
    csrfInput.value = csrfToken;
    form.appendChild(csrfInput);
    
    // Tambahkan form ke body dan submit
    document.body.appendChild(form);
    form.submit();
}

/**
 * Mendapatkan CSRF token dari halaman
 * @returns {string|null} CSRF token atau null jika tidak ditemukan
 */
function getCSRFToken() {
    // Coba ambil dari meta tag
    const metaTag = document.querySelector('meta[name="csrf-token"]');
    if (metaTag) {
        return metaTag.getAttribute('content');
    }
    
    // Coba ambil dari input hidden dengan name csrf_token
    const csrfInput = document.querySelector('input[name="csrf_token"]');
    if (csrfInput) {
        return csrfInput.value;
    }
    
    // Coba ambil dari window object jika sudah di-set
    if (window.csrfToken) {
        return window.csrfToken;
    }
    
    return null;
}

/**
 * Escape HTML untuk mencegah XSS
 * @param {string} text - Teks yang akan di-escape
 * @returns {string} Teks yang sudah di-escape
 */
function escapeHtml(text) {
    const map = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#039;'
    };
    return text.replace(/[&<>"']/g, function(m) { return map[m]; });
}

