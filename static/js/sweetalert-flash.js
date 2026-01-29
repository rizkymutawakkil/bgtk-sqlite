// SweetAlert2 Flash Messages Handler
document.addEventListener('DOMContentLoaded', function() {
    // Get all flash messages from the page
    const flashMessages = document.querySelectorAll('.flash-message');
    
    flashMessages.forEach(function(flashMessage) {
        const category = flashMessage.classList.contains('flash-success') ? 'success' :
                        flashMessage.classList.contains('flash-error') ? 'error' :
                        flashMessage.classList.contains('flash-warning') ? 'warning' :
                        flashMessage.classList.contains('flash-info') ? 'info' : 'info';
        
        const message = flashMessage.textContent.trim();
        
        // Map category to SweetAlert2 icon
        const iconMap = {
            'success': 'success',
            'error': 'error',
            'warning': 'warning',
            'info': 'info'
        };
        
        // Show SweetAlert2
        Swal.fire({
            icon: iconMap[category] || 'info',
            title: category === 'success' ? 'Berhasil!' : 
                   category === 'error' ? 'Terjadi Kesalahan!' :
                   category === 'warning' ? 'Peringatan!' : 'Informasi',
            text: message,
            confirmButtonText: 'OK',
            confirmButtonColor: '#067ac1',
            allowOutsideClick: true,
            allowEscapeKey: true
        }).then((result) => {
            // Reset form setelah notifikasi success ditutup
            if (category === 'success') {
                const form = document.querySelector('.biodata-form');
                if (form) {
                    form.reset();
                    // Reset select fields ke default
                    const selects = form.querySelectorAll('select');
                    selects.forEach(select => {
                        if (select.options.length > 0) {
                            select.selectedIndex = 0;
                        }
                    });
                    // Reset file inputs
                    const fileInputs = form.querySelectorAll('input[type="file"]');
                    fileInputs.forEach(input => {
                        input.value = '';
                        // Reset preview jika ada
                        const preview = document.getElementById(input.id + '_preview');
                        if (preview) {
                            preview.style.display = 'none';
                        }
                    });
                    // Reset signature canvas
                    const canvas = document.getElementById('ttd_canvas');
                    if (canvas) {
                        const ctx = canvas.getContext('2d');
                        ctx.fillStyle = '#FFFFFF';
                        ctx.fillRect(0, 0, canvas.width, canvas.height);
                        const signatureInput = document.getElementById('ttd');
                        if (signatureInput) {
                            signatureInput.value = '';
                        }
                    }
                    // Scroll ke atas form
                    form.scrollIntoView({ behavior: 'smooth', block: 'start' });
                }
            }
        });
        
        // Remove the flash message from DOM
        flashMessage.remove();
    });
    
    // Remove flash-messages container if empty
    const flashContainer = document.querySelector('.flash-messages');
    if (flashContainer && flashContainer.children.length === 0) {
        flashContainer.remove();
    }
});

