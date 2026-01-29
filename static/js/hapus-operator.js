/**
 * Hapus Operator Handler
 * File khusus untuk menangani hapus operator
 */

/**
 * Fungsi untuk menangani hapus operator
 * @param {number} operatorId - ID operator yang akan dihapus
 * @param {string} operatorName - Nama operator
 */
function handleHapusOperator(operatorId, operatorName) {
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
        title: 'Konfirmasi Hapus Operator',
        html: `Apakah Anda yakin ingin menghapus operator <strong>${escapeHtml(operatorName)}</strong>?<br><br><div style="background: #fee2e2; padding: 12px; border-radius: 6px; margin: 10px 0; border-left: 4px solid #ef4444;"><strong style="color: #ef4444;">⚠️ PERINGATAN:</strong><br><small style="color: #991b1b;">Tindakan ini tidak dapat dibatalkan! Semua data operator, kegiatan yang terkait, dan akses akan dihapus secara permanen.</small></div>`,
        icon: 'warning',
        showCancelButton: true,
        confirmButtonColor: '#ef4444',
        cancelButtonColor: '#6b7280',
        confirmButtonText: 'Ya, Hapus',
        cancelButtonText: 'Batal',
        reverseButtons: true,
        allowOutsideClick: false,
        allowEscapeKey: true
    }).then((result) => {
        if (result.isConfirmed) {
            // Konfirmasi kedua untuk memastikan
            Swal.fire({
                title: 'Konfirmasi Akhir',
                html: `Anda akan menghapus operator <strong>${escapeHtml(operatorName)}</strong> secara permanen.<br><br><strong style="color: #ef4444;">Apakah Anda benar-benar yakin?</strong>`,
                icon: 'error',
                showCancelButton: true,
                confirmButtonColor: '#ef4444',
                cancelButtonColor: '#6b7280',
                confirmButtonText: 'Ya, Saya Yakin',
                cancelButtonText: 'Batal',
                reverseButtons: true,
                allowOutsideClick: false,
                allowEscapeKey: true
            }).then((finalResult) => {
                if (finalResult.isConfirmed) {
                    // Submit form untuk hapus operator
                    submitHapusOperator(operatorId);
                }
            });
        }
    });
}

/**
 * Submit form untuk hapus operator
 * @param {number} operatorId - ID operator
 */
function submitHapusOperator(operatorId) {
    // Buat form untuk submit POST request
    const form = document.createElement('form');
    form.method = 'POST';
    form.action = `/admin/hapus-operator/${operatorId}`;
    
    // Tambahkan CSRF token
    const csrfInput = document.createElement('input');
    csrfInput.type = 'hidden';
    csrfInput.name = 'csrf_token';
    
    // Ambil CSRF token dari window object
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
    // Coba ambil dari window object jika sudah di-set
    if (window.csrfToken) {
        return window.csrfToken;
    }
    
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

