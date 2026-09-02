
        document.addEventListener('DOMContentLoaded', () => {
            
            // Input Elements
            const inputs = {
                companyName: document.getElementById('companyName'),
                themePrimary: document.getElementById('themeColorPrimary'),
                themeSecondary: document.getElementById('themeColorSecondary'),
                photo: document.getElementById('photoUpload'),
                name: document.getElementById('empName'),
                designation: document.getElementById('empDesignation'),
                empId: document.getElementById('empId'),
                bloodGroup: document.getElementById('empBloodGroup'),
                phone: document.getElementById('empPhone'),
                dob: document.getElementById('empDob'),
                validTill: document.getElementById('empValidTill'),
                address: document.getElementById('companyAddress'),
                website: document.getElementById('companyWebsite'),
                returnText: document.getElementById('returnText'),
                selectedTemplate: document.getElementById('selectedTemplate')
            };

            // Preview Elements
            const preview = {
                idCardFront: document.getElementById('idCardPreview'),
                idCardBack: document.getElementById('idCardPreviewBack'),
                header: document.getElementById('previewHeader'),
                headerBack: document.getElementById('previewHeaderBack'),
                companyLogo: document.getElementById('previewCompanyLogo'),
                companyLogoBack: document.getElementById('previewCompanyLogoBack'),
                companyName: document.getElementById('previewCompanyName'),
                companyNameBack: document.getElementById('previewCompanyNameBack'),
                photo: document.getElementById('previewPhoto'),
                name: document.getElementById('previewName'),
                designation: document.getElementById('previewDesignation'),
                empId: document.getElementById('previewId'),
                bloodGroup: document.getElementById('previewBlood'),
                phone: document.getElementById('previewPhone'),
                dob: document.getElementById('previewDob'),
                validTill: document.getElementById('previewValidTill'),
                address: document.getElementById('previewAddress'),
                website: document.getElementById('previewWebsite'),
                returnText: document.getElementById('previewReturnText'),
                qrCode: document.getElementById('previewQRCode'),
                sign: document.getElementById('previewSign'),
                barcode: document.getElementById('previewBarcode')
            };

            let employeeBatch = [];
            let currentPhotoSrc = preview.photo.src;
            let currentLogoSrc = null;
            let currentSignSrc = null;
            let photoOffsetX = 0;
            let photoOffsetY = 0;
            let isDraggingPhoto = false;
            let dragStartX = 0;
            let dragStartY = 0;
            let editingBatchId = null;
            let batchSearchQuery = "";
            let batchViewMode = "compact"; // "compact" or "extended"
            let bulkLogoSrc = null;
            let bulkSignSrc = null;

            // Custom Theme-Matched Alert Modal Logic
            const customAlert = document.getElementById('customAlert');
            const customAlertCard = document.getElementById('customAlertCard');
            const customAlertMessage = document.getElementById('customAlertMessage');
            const customAlertBtn = document.getElementById('customAlertBtn');
            const alertAccentBar = document.getElementById('alertAccentBar');

            function showCustomAlert(message) {
                customAlertMessage.textContent = message;
                
                // Get theme colors to make it theme-matched dynamically
                const color1 = inputs.themePrimary.value;
                const color2 = inputs.themeSecondary.value;
                
                // Update accent bar and button with the current theme colors
                alertAccentBar.style.background = `linear-gradient(90deg, ${color1} 0%, ${color2} 100%)`;
                customAlertBtn.style.background = `linear-gradient(135deg, ${color1} 0%, ${color2} 100%)`;
                customAlertBtn.style.boxShadow = `0 10px 15px -3px ${color1}40, 0 4px 6px -4px ${color1}40`;
                
                // Set custom focus ring color dynamically
                customAlertBtn.style.setProperty('--tw-ring-color', color1);
                
                // Show modal with animation
                customAlert.classList.remove('opacity-0', 'pointer-events-none');
                customAlertCard.classList.remove('scale-90', 'opacity-0');
                customAlertCard.classList.add('scale-100', 'opacity-100');
                
                // Auto focus OK button for accessibility and quick keyboard interaction (Enter to dismiss)
                setTimeout(() => customAlertBtn.focus(), 50);
            }

            function hideCustomAlert() {
                customAlert.classList.add('opacity-0', 'pointer-events-none');
                customAlertCard.classList.remove('scale-100', 'opacity-100');
                customAlertCard.classList.add('scale-90', 'opacity-0');
            }

            // Dismiss alert on click
            customAlertBtn.addEventListener('click', hideCustomAlert);
            
            // Allow clicking the backdrop to close
            customAlert.querySelector('.absolute.inset-0').addEventListener('click', hideCustomAlert);

            // Allow pressing Escape to close
            window.addEventListener('keydown', (e) => {
                if (e.key === 'Escape' && !customAlert.classList.contains('opacity-0')) {
                    hideCustomAlert();
                }
            });

            // Override window.alert
            window.alert = showCustomAlert;

            function updatePreview() {
                // Front Side
                preview.companyName.textContent = inputs.companyName.value || 'Company Name';
                preview.name.textContent = inputs.name.value || 'Employee Name';
                preview.designation.textContent = inputs.designation.value || 'Designation';
                preview.empId.textContent = inputs.empId.value || 'ID Number';
                preview.bloodGroup.textContent = inputs.bloodGroup.value || '-';
                preview.phone.textContent = inputs.phone.value || 'Phone Number';
                preview.dob.textContent = inputs.dob.value || 'DD/MM/YYYY';
                preview.validTill.textContent = inputs.validTill.value || 'DD/MM/YYYY';

                // Back Side
                preview.companyNameBack.textContent = inputs.companyName.value || 'Company Name';
                preview.address.textContent = inputs.address.value || 'Company Address';
                preview.website.textContent = inputs.website.value || 'Website';
                preview.returnText.textContent = inputs.returnText.value || `If found, please return to ${inputs.companyName.value || 'Company Name'} HQ. This card is property of the issuing company.`;

                // Logos
                if (currentLogoSrc) {
                    preview.companyLogo.src = currentLogoSrc;
                    preview.companyLogoBack.src = currentLogoSrc;
                    preview.companyLogo.classList.remove('hidden');
                    preview.companyLogoBack.classList.remove('hidden');
                } else {
                    preview.companyLogo.classList.add('hidden');
                    preview.companyLogoBack.classList.add('hidden');
                }
                
                // Signature
                if (currentSignSrc) {
                    preview.sign.src = currentSignSrc;
                    preview.sign.classList.remove('hidden');
                } else {
                    preview.sign.classList.add('hidden');
                }

                // Update Theme Colors for both sides
                const color1 = inputs.themePrimary.value;
                const color2 = inputs.themeSecondary.value;
                const gradient = `linear-gradient(90deg, ${color1} 0%, ${color2} 100%)`;
                
                preview.header.style.background = gradient;
                preview.headerBack.style.background = gradient;

                // Update CSS variables for template styling
                preview.idCardFront.style.setProperty('--theme-primary', color1);
                preview.idCardFront.style.setProperty('--theme-secondary', color2);
                preview.idCardBack.style.setProperty('--theme-primary', color1);
                preview.idCardBack.style.setProperty('--theme-secondary', color2);

                // Update QR Code
                const qrData = `Name: ${inputs.name.value || 'John Doe'}\nID: ${inputs.empId.value || 'EMP-2024-001'}\nDOB: ${inputs.dob.value || '01 Jan 1990'}\nRole: ${inputs.designation.value || 'Software Engineer'}\nPhone: ${inputs.phone.value || '99677 15150'}\nBlood Group: ${inputs.bloodGroup.value || 'O+'}`;
                const qr = new QRious({
                    value: qrData,
                    size: 256,
                    level: 'M'
                });
                preview.qrCode.src = qr.toDataURL();

                // Update Barcode
                if (window.JsBarcode) {
                    const barcodeValue = inputs.empId.value || 'EMP-2024-001';
                    const canvas = document.createElement('canvas');
                    JsBarcode(canvas, barcodeValue, {
                        format: "CODE128",
                        displayValue: false,
                        height: 30,
                        width: 1.5,
                        margin: 0
                    });
                    preview.barcode.src = canvas.toDataURL();
                }

                // Update "Print Card" button active/disabled state
                const isFormComplete = 
                    inputs.companyName.value.trim() !== '' &&
                    !currentPhotoSrc.startsWith('data:image/svg+xml') &&
                    inputs.name.value.trim() !== '' &&
                    inputs.designation.value.trim() !== '' &&
                    inputs.empId.value.trim() !== '' &&
                    inputs.bloodGroup.value !== '' &&
                    inputs.phone.value.length === 10 &&
                    inputs.dob.value.length === 10 &&
                    inputs.validTill.value.trim() !== '' &&
                    inputs.address.value.trim() !== '' &&
                    inputs.website.value.trim() !== '';

                const printPdfBtn = document.getElementById('printPdfBtn');
                const downloadPdfBtn = document.getElementById('downloadPdfBtn');
                if (isFormComplete) {
                    printPdfBtn.removeAttribute('disabled');
                    if (downloadPdfBtn) downloadPdfBtn.removeAttribute('disabled');
                } else {
                    printPdfBtn.setAttribute('disabled', 'true');
                    if (downloadPdfBtn) downloadPdfBtn.setAttribute('disabled', 'true');
                }
            }

            function handleImageUpload(event) {
                const file = event.target.files[0];
                if (file) {
                    if (!file.type.startsWith('image/')) {
                        alert('Please select a valid image file.');
                        return;
                    }
                    const reader = new FileReader();
                    reader.onload = function(e) {
                        currentPhotoSrc = e.target.result;
                        preview.photo.src = currentPhotoSrc;
                        photoOffsetX = 0;
                        photoOffsetY = 0;
                        preview.photo.style.objectPosition = '50% 50%';
                        
                        const photoUploadText = document.getElementById('photoUploadText');
                        if (photoUploadText) {
                            photoUploadText.textContent = `Selected: ${file.name.substring(0, 18)}${file.name.length > 18 ? '...' : ''}`;
                            photoUploadText.className = "text-xs font-semibold text-green-600";
                        }
                        
                        updatePreview();
                    };
                    reader.readAsDataURL(file);
                }
            }

            function handleLogoUpload(event) {
                const file = event.target.files[0];
                if (file) {
                    if (!file.type.startsWith('image/')) {
                        alert('Please select a valid image file.');
                        return;
                    }
                    const reader = new FileReader();
                    reader.onload = function(e) {
                        currentLogoSrc = e.target.result;
                        
                        const logoUploadText = document.getElementById('logoUploadText');
                        if (logoUploadText) {
                            logoUploadText.textContent = `Selected: ${file.name.substring(0, 18)}${file.name.length > 18 ? '...' : ''}`;
                            logoUploadText.className = "text-xs font-semibold text-green-600";
                        }
                        
                        updatePreview();
                    };
                    reader.readAsDataURL(file);
                }
            }

            function handleSignUpload(event) {
                const file = event.target.files[0];
                if (file) {
                    if (!file.type.startsWith('image/')) {
                        alert('Please select a valid image file.');
                        return;
                    }
                    const reader = new FileReader();
                    reader.onload = function(e) {
                        currentSignSrc = e.target.result;
                        
                        const signUploadText = document.getElementById('signUploadText');
                        if (signUploadText) {
                            signUploadText.textContent = `Selected: ${file.name.substring(0, 18)}${file.name.length > 18 ? '...' : ''}`;
                            signUploadText.className = "text-xs font-semibold text-green-600";
                        }
                        
                        updatePreview();
                    };
                    reader.readAsDataURL(file);
                }
            }

            document.getElementById('companyLogoUpload').addEventListener('change', handleLogoUpload);
            document.getElementById('signUpload').addEventListener('change', handleSignUpload);

            // Template Selection Interactive Logic
            const templateBtns = document.querySelectorAll('.template-btn');
            
            templateBtns.forEach(btn => {
                btn.addEventListener('click', () => {
                    const template = btn.getAttribute('data-template');
                    inputs.selectedTemplate.value = template;

                    // Update active button layout states
                    templateBtns.forEach(b => {
                        b.className = 'template-btn border-2 border-gray-200 bg-white hover:bg-gray-50 text-gray-700 px-3 py-2 rounded-xl flex flex-col items-center justify-center transition duration-200 focus:outline-none';
                    });
                    btn.className = 'template-btn border-2 border-indigo-600 bg-indigo-50/50 text-indigo-900 px-3 py-2 rounded-xl flex flex-col items-center justify-center transition duration-200 shadow-sm focus:outline-none';

                    // Update front/back card element template classes
                    const templates = ['classic', 'modern', 'dark', 'gradient', 'luxury', 'aurora', 'emerald', 'carbon'];
                    templates.forEach(t => {
                        preview.idCardFront.classList.remove(`template-${t}`);
                        preview.idCardBack.classList.remove(`template-${t}`);
                    });
                    
                    preview.idCardFront.classList.add(`template-${template}`);
                    preview.idCardBack.classList.add(`template-${template}`);
                    
                    updatePreview();
                });
            });

            // Photo Dragging Logic
            preview.photo.style.cursor = 'move';
            
            preview.photo.addEventListener('mousedown', (e) => {
                isDraggingPhoto = true;
                dragStartX = e.clientX;
                dragStartY = e.clientY;
                e.preventDefault();
            });

            window.addEventListener('mousemove', (e) => {
                if (!isDraggingPhoto) return;
                const dx = e.clientX - dragStartX;
                const dy = e.clientY - dragStartY;
                photoOffsetX += dx;
                photoOffsetY += dy;
                dragStartX = e.clientX;
                dragStartY = e.clientY;
                preview.photo.style.objectPosition = `calc(50% + ${photoOffsetX}px) calc(50% + ${photoOffsetY}px)`;
            });

            window.addEventListener('mouseup', () => {
                isDraggingPhoto = false;
            });
            
            // Add touch support for mobile dragging
            preview.photo.addEventListener('touchstart', (e) => {
                isDraggingPhoto = true;
                dragStartX = e.touches[0].clientX;
                dragStartY = e.touches[0].clientY;
            }, { passive: false });

            window.addEventListener('touchmove', (e) => {
                if (!isDraggingPhoto) return;
                e.preventDefault(); // Prevent scrolling while dragging photo
                const dx = e.touches[0].clientX - dragStartX;
                const dy = e.touches[0].clientY - dragStartY;
                photoOffsetX += dx;
                photoOffsetY += dy;
                dragStartX = e.touches[0].clientX;
                dragStartY = e.touches[0].clientY;
                preview.photo.style.objectPosition = `calc(50% + ${photoOffsetX}px) calc(50% + ${photoOffsetY}px)`;
            }, { passive: false });

            window.addEventListener('touchend', () => {
                isDraggingPhoto = false;
            });

            // Date formatting and automatic correction helper
            function formatAndCorrectDate(val) {
                let trimmed = val.trim();
                if (trimmed && /^[A-Za-z]/.test(trimmed)) {
                    return val;
                }
                
                // Convert ISO YYYY-MM-DD or YYYY/MM/DD format to DD/MM/YYYY
                const isoMatch = trimmed.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})/);
                if (isoMatch) {
                    const y = isoMatch[1];
                    const m = isoMatch[2].padStart(2, '0');
                    const d = isoMatch[3].padStart(2, '0');
                    trimmed = `${d}/${m}/${y}`;
                }
                
                let digits = trimmed.replace(/\D/g, '').substring(0, 8);
                let day = '';
                let month = '';
                let year = '';
                
                if (digits.length > 0) {
                    day = digits.substring(0, 2);
                    if (day.length === 2) {
                        let d = parseInt(day, 10);
                        if (d > 31) day = '31';
                        if (d === 0) day = '01';
                    }
                }
                if (digits.length > 2) {
                    month = digits.substring(2, 4);
                    if (month.length === 2) {
                        let m = parseInt(month, 10);
                        if (m > 12) month = '12';
                        if (m === 0) month = '01';
                    }
                }
                if (digits.length > 4) {
                    year = digits.substring(4, 8);
                }
                
                let formatted = day;
                if (month) {
                    formatted += '/' + month;
                } else if (digits.length > 2) {
                    formatted += '/';
                }
                if (year) {
                    formatted += '/' + year;
                } else if (digits.length > 4) {
                    formatted += '/';
                }
                return formatted;
            }

            // Add Event Listeners for real-time updates
            Object.values(inputs).forEach(input => {
                if(input.id === 'photoUpload') {
                    input.addEventListener('change', handleImageUpload);
                } else if(input.id === 'empBloodGroup') {
                    input.addEventListener('change', updatePreview);
                } else if(input.id === 'empPhone') {
                    input.addEventListener('input', (e) => {
                        e.target.value = e.target.value.replace(/\D/g, '').substring(0, 10);
                        updatePreview();
                    });
                } else if(input.id === 'empId') {
                    input.addEventListener('input', (e) => {
                        e.target.value = e.target.value.toUpperCase();
                        updatePreview();
                    });
                } else if(input.id === 'empDob') {
                    input.addEventListener('input', (e) => {
                        e.target.value = formatAndCorrectDate(e.target.value);
                        updatePreview();
                    });
                } else if(input.id === 'empValidTill') {
                    input.addEventListener('input', (e) => {
                        e.target.value = formatAndCorrectDate(e.target.value);
                        updatePreview();
                    });
                } else if(input.id === 'empName' || input.id === 'companyAddress') {
                    input.addEventListener('input', (e) => {
                        // Capitalize first letter of each word
                        e.target.value = e.target.value.replace(/\b\w/g, char => char.toUpperCase());
                        updatePreview();
                    });
                } else if(input.id === 'companyName') {
                    input.addEventListener('input', (e) => {
                        e.target.value = e.target.value.toUpperCase();
                        updatePreview();
                    });
                } else if(input.id === 'companyWebsite') {
                    input.addEventListener('input', (e) => {
                        let val = e.target.value.toLowerCase();
                        val = val.replace(/^https?:\/\//, ''); // Remove http(s)://
                        val = val.split('/')[0]; // Remove paths
                        val = val.replace(/[^a-z0-9.-]/g, ''); // Remove invalid domain chars
                        e.target.value = val;
                        updatePreview();
                    });
                } else {
                    input.addEventListener('input', updatePreview);
                }
            });

            const addToBatchBtn = document.getElementById('addToBatchBtn');
            const batchList = document.getElementById('batchList');
            const batchCount = document.getElementById('batchCount');
            const emptyBatchMsg = document.getElementById('emptyBatchMsg');
            const printBatchBtn = document.getElementById('printBatchBtn');

            function updateBatchUI() {
                batchCount.textContent = employeeBatch.length;

                const query = batchSearchQuery.trim().toLowerCase();
                const filteredBatch = employeeBatch.filter(emp => {
                    if (!query) return true;
                    return (
                        (emp.name || "").toLowerCase().includes(query) ||
                        (emp.empId || "").toLowerCase().includes(query) ||
                        (emp.designation || "").toLowerCase().includes(query) ||
                        (emp.companyName || "").toLowerCase().includes(query) ||
                        (emp.bloodGroup || "").toLowerCase().includes(query) ||
                        (emp.phone || "").toLowerCase().includes(query) ||
                        (emp.dob || "").toLowerCase().includes(query) ||
                        (emp.validTill || "").toLowerCase().includes(query) ||
                        (emp.address || "").toLowerCase().includes(query) ||
                        (emp.website || "").toLowerCase().includes(query) ||
                        (emp.template || "").toLowerCase().includes(query)
                    );
                });

                batchList.innerHTML = '';

                const downloadBatchBtn = document.getElementById('downloadBatchBtn');
                
                if (employeeBatch.length === 0) {
                    emptyBatchMsg.style.display = 'block';
                    printBatchBtn.disabled = true;
                    if (downloadBatchBtn) downloadBatchBtn.disabled = true;
                    batchList.appendChild(emptyBatchMsg);
                } else if (filteredBatch.length === 0) {
                    emptyBatchMsg.style.display = 'none';
                    printBatchBtn.disabled = true;
                    if (downloadBatchBtn) downloadBatchBtn.disabled = true;
                    
                    const noResults = document.createElement('div');
                    noResults.className = 'text-xs text-slate-500 italic text-center py-6 bg-slate-50/50 rounded-xl border border-slate-100 flex items-center justify-center gap-2';
                    noResults.innerHTML = `<span>🔍 No matching cards found for "<strong>${batchSearchQuery}</strong>"</span>`;
                    batchList.appendChild(noResults);
                } else {
                    emptyBatchMsg.style.display = 'none';
                    printBatchBtn.disabled = false;
                    if (downloadBatchBtn) downloadBatchBtn.disabled = false;

                    filteredBatch.forEach(emp => {
                        const item = document.createElement('div');
                        
                        if (batchViewMode === "extended") {
                            // Detailed Extended layout
                            item.className = 'flex flex-col p-4 bg-white/80 border border-slate-200/80 rounded-2xl shadow-sm hover:border-indigo-200/80 hover:bg-white transition-all gap-3.5';
                            item.innerHTML = `
                                <!-- Top profile summary row -->
                                <div class="flex items-center justify-between pb-3 border-b border-slate-100">
                                    <div class="flex items-center">
                                        <img src="${emp.photoSrc}" class="w-11 h-11 rounded-xl object-cover border border-slate-200/80 mr-3 shadow-sm">
                                        <div>
                                            <p class="text-sm font-bold text-slate-800 leading-tight">${emp.name}</p>
                                            <div class="flex flex-wrap gap-1 mt-0.5">
                                                <span class="inline-flex items-center px-2 py-0.5 rounded-md text-[10px] font-bold bg-indigo-50 text-indigo-700 border border-indigo-100/50 uppercase leading-none">${emp.empId}</span>
                                                <span class="inline-flex items-center px-2 py-0.5 rounded-md text-[10px] font-semibold bg-slate-50 text-slate-500 border border-slate-200/50 leading-none capitalize">${emp.template} template</span>
                                            </div>
                                        </div>
                                    </div>
                                    <div class="flex gap-2">
                                        <button type="button" onclick="editFromBatch('${emp.batchId}')" class="p-2 bg-indigo-50 text-indigo-600 hover:bg-indigo-100 border border-indigo-100/50 hover:border-indigo-200 rounded-xl transition-all duration-200 transform hover:scale-105 active:scale-95 shadow-sm focus:outline-none" title="Edit Employee">
                                            <svg class="w-4 h-4" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z"></path></svg>
                                        </button>
                                        <button type="button" onclick="removeFromBatch('${emp.batchId}')" class="p-2 bg-rose-50 text-rose-600 hover:bg-rose-100 border border-rose-100/50 hover:border-rose-200 rounded-xl transition-all duration-200 transform hover:scale-105 active:scale-95 shadow-sm focus:outline-none" title="Remove Employee">
                                            <svg class="w-4 h-4" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"></path></svg>
                                        </button>
                                    </div>
                                </div>
                                
                                <!-- Detailed details grid -->
                                <div class="grid grid-cols-2 sm:grid-cols-3 gap-y-2.5 gap-x-3 text-[11px] text-slate-600 bg-slate-50/60 p-3 rounded-xl border border-slate-100/60">
                                    <div>
                                        <span class="text-slate-400 block text-[9px] uppercase tracking-wider font-semibold leading-none mb-1">Designation</span>
                                        <span class="font-semibold text-slate-700">${emp.designation || 'N/A'}</span>
                                    </div>
                                    <div>
                                        <span class="text-slate-400 block text-[9px] uppercase tracking-wider font-semibold leading-none mb-1">Company</span>
                                        <span class="font-semibold text-slate-700">${emp.companyName || 'N/A'}</span>
                                    </div>
                                    <div>
                                        <span class="text-slate-400 block text-[9px] uppercase tracking-wider font-semibold leading-none mb-1">Phone</span>
                                        <span class="font-semibold text-slate-700">${emp.phone || 'N/A'}</span>
                                    </div>
                                    <div>
                                        <span class="text-slate-400 block text-[9px] uppercase tracking-wider font-semibold leading-none mb-1">Blood Group</span>
                                        <span class="font-bold text-red-600">${emp.bloodGroup || 'N/A'}</span>
                                    </div>
                                    <div>
                                        <span class="text-slate-400 block text-[9px] uppercase tracking-wider font-semibold leading-none mb-1">D.O.B</span>
                                        <span class="font-semibold text-slate-700">${emp.dob || 'N/A'}</span>
                                    </div>
                                    <div>
                                        <span class="text-slate-400 block text-[9px] uppercase tracking-wider font-semibold leading-none mb-1">Valid Till</span>
                                        <span class="font-semibold text-slate-700">${emp.validTill || 'N/A'}</span>
                                    </div>
                                </div>
                            `;
                        } else {
                            // Compact/Original layout
                            item.className = 'flex items-center justify-between p-3.5 bg-white/80 border border-slate-200/80 rounded-2xl shadow-sm hover:border-indigo-200 hover:bg-white transition-all duration-300 transform hover:scale-[1.005]';
                            item.innerHTML = `
                                <div class="flex items-center">
                                    <img src="${emp.photoSrc}" class="w-10 h-10 rounded-full object-cover border border-slate-200 mr-3.5 shadow-sm">
                                    <div>
                                        <p class="text-sm font-bold text-slate-800 leading-tight">${emp.name}</p>
                                        <p class="text-xs text-slate-500 font-medium mt-0.5">${emp.empId} • ${emp.designation}</p>
                                    </div>
                                </div>
                                <div class="flex gap-2">
                                    <button type="button" onclick="editFromBatch('${emp.batchId}')" class="p-2 bg-indigo-50 text-indigo-600 hover:bg-indigo-100 border border-indigo-100/50 hover:border-indigo-200 rounded-xl transition-all duration-200 transform hover:scale-105 active:scale-95 focus:outline-none" title="Edit Employee">
                                        <svg class="w-4 h-4" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M11 5H6a2 2 0 00-2 2v11a2 2 0 002 2h11a2 2 0 002-2v-5m-1.414-9.414a2 2 0 112.828 2.828L11.828 15H9v-2.828l8.586-8.586z"></path></svg>
                                    </button>
                                    <button type="button" onclick="removeFromBatch('${emp.batchId}')" class="p-2 bg-rose-50 text-rose-600 hover:bg-rose-100 border border-rose-100/50 hover:border-rose-200 rounded-xl transition-all duration-200 transform hover:scale-105 active:scale-95 focus:outline-none" title="Remove Employee">
                                        <svg class="w-4 h-4" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"></path></svg>
                                    </button>
                                </div>
                            `;
                        }
                        batchList.appendChild(item);
                    });
                }
            }

            // --- Batch Search and View Mode Selectors listeners ---
            const batchSearchInput = document.getElementById('batchSearchInput');
            const clearBatchSearchBtn = document.getElementById('clearBatchSearchBtn');
            const viewCompactBtn = document.getElementById('viewCompactBtn');
            const viewExtendedBtn = document.getElementById('viewExtendedBtn');

            if (batchSearchInput) {
                batchSearchInput.addEventListener('input', (e) => {
                    batchSearchQuery = e.target.value;
                    if (batchSearchQuery.length > 0) {
                        clearBatchSearchBtn.classList.remove('hidden');
                    } else {
                        clearBatchSearchBtn.classList.add('hidden');
                    }
                    updateBatchUI();
                });
            }

            if (clearBatchSearchBtn) {
                clearBatchSearchBtn.addEventListener('click', () => {
                    batchSearchInput.value = "";
                    batchSearchQuery = "";
                    clearBatchSearchBtn.classList.add('hidden');
                    updateBatchUI();
                });
            }

            if (viewCompactBtn && viewExtendedBtn) {
                viewCompactBtn.addEventListener('click', () => {
                    batchViewMode = "compact";
                    viewCompactBtn.className = 'px-3 py-1.5 rounded-md text-[11px] font-semibold transition-all duration-200 bg-white text-slate-800 shadow-sm flex items-center gap-1 border border-slate-200/40';
                    viewExtendedBtn.className = 'px-3 py-1.5 rounded-md text-[11px] font-semibold transition-all duration-200 text-slate-600 hover:text-slate-800 flex items-center gap-1';
                    updateBatchUI();
                });

                viewExtendedBtn.addEventListener('click', () => {
                    batchViewMode = "extended";
                    viewExtendedBtn.className = 'px-3 py-1.5 rounded-md text-[11px] font-semibold transition-all duration-200 bg-white text-slate-800 shadow-sm flex items-center gap-1 border border-slate-200/40';
                    viewCompactBtn.className = 'px-3 py-1.5 rounded-md text-[11px] font-semibold transition-all duration-200 text-slate-600 hover:text-slate-800 flex items-center gap-1';
                    updateBatchUI();
                });
            }

            window.removeFromBatch = function(batchId) {
                employeeBatch = employeeBatch.filter(emp => emp.batchId !== batchId);
                updateBatchUI();
            };

            window.editFromBatch = function(batchId) {
                const emp = employeeBatch.find(e => e.batchId === batchId);
                if (!emp) return;

                // Automatically expand all sidebar sections so inputs are visible for editing
                expandSection('templateSelectorContainer');
                expandSection('companyInfoContainer');
                expandSection('employeeDetailsContainer');
                expandSection('backSideDetailsContainer');

                // Populate Inputs
                inputs.companyName.value = emp.companyName !== 'Company Name' ? emp.companyName : '';
                inputs.themePrimary.value = emp.themePrimary;
                inputs.themeSecondary.value = emp.themeSecondary;
                inputs.name.value = emp.name !== 'Employee Name' ? emp.name : '';
                inputs.designation.value = emp.designation !== 'Designation' ? emp.designation : '';
                inputs.empId.value = emp.empId !== 'ID Number' ? emp.empId : '';
                inputs.bloodGroup.value = emp.bloodGroup !== '-' ? emp.bloodGroup : 'O+';
                inputs.phone.value = emp.phone !== 'Phone Number' ? emp.phone : '';
                inputs.dob.value = emp.dob !== 'DD/MM/YYYY' ? emp.dob : '';
                inputs.validTill.value = emp.validTill && emp.validTill !== 'Permanent' && emp.validTill !== 'DD/MM/YYYY' ? emp.validTill : '';
                inputs.address.value = emp.address !== 'Company Address' ? emp.address : '';
                inputs.website.value = emp.website !== 'Website' ? emp.website : '';
                inputs.returnText.value = emp.returnText !== '' ? emp.returnText : '';

                // Restore image state
                currentPhotoSrc = emp.photoSrc;
                photoOffsetX = emp.photoOffsetX || 0;
                photoOffsetY = emp.photoOffsetY || 0;
                preview.photo.src = currentPhotoSrc;
                preview.photo.style.objectPosition = `calc(50% + ${photoOffsetX}px) calc(50% + ${photoOffsetY}px)`;
                currentLogoSrc = emp.logoSrc;
                currentSignSrc = emp.signSrc || null;

                // Sync custom image selector text labels on edit
                const photoUploadText = document.getElementById('photoUploadText');
                if (photoUploadText) {
                    if (emp.photoSrc && !emp.photoSrc.startsWith('data:image/svg+xml')) {
                        photoUploadText.textContent = `Photo Active (Adjustable)`;
                        photoUploadText.className = "text-xs font-semibold text-green-600";
                    } else {
                        photoUploadText.textContent = "Upload Employee Photo";
                        photoUploadText.className = "text-xs font-semibold text-slate-600";
                    }
                }
                const logoUploadText = document.getElementById('logoUploadText');
                if (logoUploadText) {
                    if (emp.logoSrc) {
                        logoUploadText.textContent = `Logo Active`;
                        logoUploadText.className = "text-xs font-semibold text-green-600";
                    } else {
                        logoUploadText.textContent = "Upload Company Logo";
                        logoUploadText.className = "text-xs font-semibold text-slate-600";
                    }
                }
                const signUploadText = document.getElementById('signUploadText');
                if (signUploadText) {
                    if (emp.signSrc) {
                        signUploadText.textContent = `Signature Active`;
                        signUploadText.className = "text-xs font-semibold text-green-600";
                    } else {
                        signUploadText.textContent = "Upload MD Signature";
                        signUploadText.className = "text-xs font-semibold text-slate-600";
                    }
                }

                // Restore template style selection state
                const template = emp.template || 'classic';
                inputs.selectedTemplate.value = template;
                
                const tBtns = document.querySelectorAll('.template-btn');
                tBtns.forEach(btn => {
                    const t = btn.getAttribute('data-template');
                    if (t === template) {
                        btn.className = 'template-btn border-2 border-indigo-600 bg-indigo-50/50 text-indigo-900 px-3 py-2 rounded-xl flex flex-col items-center justify-center transition duration-200 shadow-sm focus:outline-none';
                    } else {
                        btn.className = 'template-btn border-2 border-gray-200 bg-white hover:bg-gray-50 text-gray-700 px-3 py-2 rounded-xl flex flex-col items-center justify-center transition duration-200 focus:outline-none';
                    }
                });

                const templatesList = ['classic', 'modern', 'dark', 'gradient', 'luxury', 'aurora', 'emerald', 'carbon'];
                templatesList.forEach(t => {
                    preview.idCardFront.classList.remove(`template-${t}`);
                    preview.idCardBack.classList.remove(`template-${t}`);
                });
                preview.idCardFront.classList.add(`template-${template}`);
                preview.idCardBack.classList.add(`template-${template}`);

                // Set edit mode
                editingBatchId = batchId;
                
                // Update button UI
                addToBatchBtn.innerHTML = `<svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"></path></svg> Update Employee`;
                addToBatchBtn.className = 'w-full bg-yellow-50 border border-yellow-300 hover:bg-yellow-100 text-yellow-700 font-semibold py-3 px-4 rounded-xl transition duration-200 flex justify-center items-center text-sm shadow-sm active:scale-[0.98] transform';

                // Update saveToBatchBtn UI
                const saveToBatchBtn = document.getElementById('saveToBatchBtn');
                const saveToBatchText = document.getElementById('saveToBatchText');
                if (saveToBatchBtn && saveToBatchText) {
                    saveToBatchText.textContent = "Update Card";
                    saveToBatchBtn.className = 'bg-gradient-to-r from-amber-500 to-orange-500 hover:from-amber-600 hover:to-orange-600 text-white font-semibold py-2.5 px-5 rounded-xl shadow-md shadow-amber-100 hover:shadow-lg hover:shadow-amber-200 transition-all duration-200 flex items-center transform hover:scale-[1.02] active:scale-[0.98] focus:outline-none focus:ring-2 focus:ring-amber-500 focus:ring-offset-2';
                }

                updatePreview();
                window.scrollTo({ top: 0, behavior: 'smooth' });
            };

            addToBatchBtn.addEventListener('click', () => {
                // Validation
                if (!inputs.companyName.value.trim()) return alert("Please enter Company Name.");
                if (currentPhotoSrc.startsWith('data:image/svg+xml')) return alert("Please upload an Employee Photo.");
                if (!inputs.name.value.trim()) return alert("Please enter Full Name.");
                if (!inputs.designation.value.trim()) return alert("Please enter Designation.");
                if (!inputs.empId.value.trim()) return alert("Please enter Employee ID.");
                if (!inputs.bloodGroup.value) return alert("Please select Blood Group.");                if (inputs.phone.value.length < 10) return alert("Please enter a valid 10-digit Phone Number.");
                if (inputs.dob.value.length < 10) return alert("Please enter a valid Date of Birth (DD/MM/YYYY).");
                if (!inputs.validTill.value.trim()) return alert("Please enter Date of Joining.");
                if (!inputs.address.value.trim()) return alert("Please enter Company Address.");
                if (!inputs.website.value.trim()) return alert("Please enter Website.");
 
                const cardData = {
                    batchId: editingBatchId || Date.now().toString(),
                    companyName: inputs.companyName.value || 'Company Name',
                    themePrimary: inputs.themePrimary.value,
                    themeSecondary: inputs.themeSecondary.value,
                    name: inputs.name.value || 'Employee Name',
                    designation: inputs.designation.value || 'Designation',
                    empId: inputs.empId.value || 'ID Number',
                    bloodGroup: inputs.bloodGroup.value || '-',
                    phone: inputs.phone.value || 'Phone Number',
                    dob: inputs.dob.value || 'DD/MM/YYYY',
                    validTill: inputs.validTill.value || 'DD/MM/YYYY',
                    address: inputs.address.value || 'Company Address',
                    website: inputs.website.value || 'Website',
                    returnText: inputs.returnText.value || `If found, please return to ${inputs.companyName.value || 'Company Name'} HQ. This card is property of the issuing company.`,
                    photoSrc: currentPhotoSrc,
                    photoOffsetX: photoOffsetX,
                    photoOffsetY: photoOffsetY,
                    qrCodeSrc: preview.qrCode.src,
                    barcodeSrc: preview.barcode.src,
                    logoSrc: currentLogoSrc,
                    signSrc: currentSignSrc,
                    template: inputs.selectedTemplate.value || 'classic'
                };

                const wasEditing = !!editingBatchId;
                let targetIndex = -1;
                
                if (editingBatchId) {
                    targetIndex = employeeBatch.findIndex(e => e.batchId === editingBatchId);
                }
                
                // Fallback check: look for any card in the batch with the same Employee ID to prevent duplicate IDs
                if (targetIndex === -1 && cardData.empId) {
                    targetIndex = employeeBatch.findIndex(e => e.empId.toUpperCase() === cardData.empId.toUpperCase());
                }

                if (targetIndex !== -1) {
                    // Keep the original batchId to preserve UI list key mapping references
                    cardData.batchId = employeeBatch[targetIndex].batchId;
                    employeeBatch[targetIndex] = cardData;
                    editingBatchId = null;
                    addToBatchBtn.innerHTML = `<svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" stroke-width="2.5" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7"></path></svg> Updated!`;
                } else {
                    employeeBatch.push(cardData);
                    addToBatchBtn.innerHTML = `<svg class="w-4 h-4 mr-2" fill="none" stroke="currentColor" stroke-width="2.5" viewBox="0 0 24 24" xmlns="http://www.w3.org/2000/svg"><path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7"></path></svg> Added!`;
                }
                
                updateBatchUI();
                addToBatchBtn.className = 'w-full bg-green-50 border border-green-300 hover:bg-green-100 text-green-700 font-semibold py-3 px-4 rounded-xl transition duration-200 flex justify-center items-center text-sm shadow-sm active:scale-[0.98] transform';
                
                const saveToBatchBtn = document.getElementById('saveToBatchBtn');
                const saveToBatchText = document.getElementById('saveToBatchText');
                if (saveToBatchBtn && saveToBatchText) {
                    saveToBatchText.textContent = wasEditing ? "Updated!" : "Added!";
                    saveToBatchBtn.className = 'bg-gradient-to-r from-green-600 to-emerald-600 hover:from-green-700 hover:to-emerald-700 text-white font-semibold py-2.5 px-5 rounded-xl shadow-md shadow-green-100 hover:shadow-lg hover:shadow-green-200 transition-all duration-200 flex items-center transform hover:scale-[1.02] active:scale-[0.98] focus:outline-none focus:ring-2 focus:ring-green-500 focus:ring-offset-2';
                }

                setTimeout(() => {
                    if (editingBatchId) return;
                    addToBatchBtn.innerHTML = `<span class="text-lg mr-1.5 font-medium leading-none">+</span> Add Employee to Batch`;
                    addToBatchBtn.className = 'w-full bg-indigo-50/50 border border-indigo-200 hover:bg-indigo-100/70 text-indigo-600 font-semibold py-3 px-4 rounded-xl transition duration-200 flex justify-center items-center text-sm shadow-sm hover:shadow active:scale-[0.98] transform';
                    
                    if (saveToBatchBtn && saveToBatchText) {
                        saveToBatchText.textContent = "Save to Batch";
                        saveToBatchBtn.className = 'bg-gradient-to-r from-emerald-600 to-teal-600 hover:from-emerald-700 hover:to-teal-700 text-white font-semibold py-2.5 px-5 rounded-xl shadow-md shadow-emerald-100 hover:shadow-lg hover:shadow-emerald-200 transition-all duration-200 flex items-center transform hover:scale-[1.02] active:scale-[0.98] focus:outline-none focus:ring-2 focus:ring-emerald-500 focus:ring-offset-2';
                    }
                }, 1500);
            });

            // Bind saveToBatchBtn click event to addToBatchBtn click
            const saveToBatchBtn = document.getElementById('saveToBatchBtn');
            if (saveToBatchBtn) {
                saveToBatchBtn.addEventListener('click', () => {
                    addToBatchBtn.click();
                });
            }

            // --- NATIVE BROWSER PRINTING LOGIC ---

            // Helper function to scale the high-res 324px card down to exact physical 51.5x82mm dimensions
            function createPrintableCard(htmlContent) {
                const wrapper = document.createElement('div');
                wrapper.className = 'printable-card-wrapper';
                wrapper.style.width = '51.5mm';
                wrapper.style.height = '82mm';
                wrapper.style.position = 'relative';
                wrapper.style.overflow = 'hidden';
                wrapper.style.pageBreakInside = 'avoid';
                wrapper.style.breakInside = 'avoid';
                
                const scaler = document.createElement('div');
                // 324px mapped to 51.5mm (approx 194.4px). Scale factor is 0.60
                scaler.style.transform = 'scale(0.60)';
                scaler.style.transformOrigin = 'top left';
                scaler.style.width = '324px';
                scaler.style.height = '516px';
                
                scaler.innerHTML = htmlContent;
                wrapper.appendChild(scaler);
                
                return wrapper;
            }

            // Slices cards into groups of 9 and wraps each group in a .print-page A4 wrapper
            function renderPrintPages(cardsArray) {
                const printArea = document.getElementById('printArea');
                printArea.innerHTML = '';
                
                const cardsPerPage = 9;
                for (let i = 0; i < cardsArray.length; i += cardsPerPage) {
                    const pageDiv = document.createElement('div');
                    pageDiv.className = 'print-page';
                    
                    const chunk = cardsArray.slice(i, i + cardsPerPage);
                    chunk.forEach(cardWrapper => {
                        pageDiv.appendChild(cardWrapper);
                    });
                    
                    printArea.appendChild(pageDiv);
                }
            }

            // --- HIGH-FIDELITY PDF EXPORTING SYSTEM ---

            function downloadPdfFromArea(title, cardsArray) {
                const overlay = document.getElementById('loadingOverlay');
                if (overlay) {
                    overlay.classList.remove('hidden');
                }
                
                const tempContainer = document.createElement('div');
                tempContainer.style.width = '210mm';
                tempContainer.style.background = 'white';
                
                const cardsPerPage = 9;
                for (let i = 0; i < cardsArray.length; i += cardsPerPage) {
                    const pageDiv = document.createElement('div');
                    pageDiv.className = 'print-page';
                    pageDiv.style.width = '210mm';
                    pageDiv.style.height = '297mm';
                    pageDiv.style.display = 'flex';
                    pageDiv.style.flexWrap = 'wrap';
                    pageDiv.style.gap = '6mm 8mm';
                    pageDiv.style.justifyContent = 'center';
                    pageDiv.style.alignContent = 'center';
                    pageDiv.style.boxSizing = 'border-box';
                    pageDiv.style.padding = '10mm 15mm';
                    pageDiv.style.pageBreakInside = 'avoid';
                    pageDiv.style.breakInside = 'avoid';
                    pageDiv.style.background = 'white';
                    
                    if (i + cardsPerPage < cardsArray.length) {
                        pageDiv.style.pageBreakAfter = 'always';
                        pageDiv.style.breakAfter = 'always';
                    }
                    
                    const chunk = cardsArray.slice(i, i + cardsPerPage);
                    chunk.forEach(cardWrapper => {
                        pageDiv.appendChild(cardWrapper.cloneNode(true));
                    });
                    
                    tempContainer.appendChild(pageDiv);
                }
                
                tempContainer.style.position = 'absolute';
                tempContainer.style.left = '-9999px';
                tempContainer.style.top = '0';
                document.body.appendChild(tempContainer);
                
                const opt = {
                    margin:       0,
                    filename:     `${title.toLowerCase().replace(/\s+/g, '_')}_cards.pdf`,
                    image:        { type: 'jpeg', quality: 0.98 },
                    html2canvas:  { scale: 2, useCORS: true, logging: false },
                    jsPDF:        { unit: 'mm', format: 'a4', orientation: 'portrait' }
                };
                
                html2pdf().from(tempContainer).set(opt).save().then(() => {
                    document.body.removeChild(tempContainer);
                    if (overlay) {
                        overlay.classList.add('hidden');
                    }
                }).catch(err => {
                    console.error(err);
                    if (tempContainer.parentNode) document.body.removeChild(tempContainer);
                    if (overlay) {
                        overlay.classList.add('hidden');
                    }
                    alert('An error occurred while compiling the PDF document.');
                });
            }

            // Click listener for single card PDF download
            document.getElementById('downloadPdfBtn').addEventListener('click', () => {
                const frontClone = document.getElementById('idCardPreview').cloneNode(true);
                const backClone = document.getElementById('idCardPreviewBack').cloneNode(true);
                
                const card1 = createPrintableCard(frontClone.outerHTML);
                const card2 = createPrintableCard(backClone.outerHTML);
                
                const empName = inputs.name.value.trim() || 'Employee';
                downloadPdfFromArea(empName, [card1, card2]);
            });

            // Click listener for batch cards PDF download
            document.getElementById('downloadBatchBtn').addEventListener('click', () => {
                if (employeeBatch.length === 0) return;
                
                const cardsList = [];
                employeeBatch.forEach((emp) => {
                    const frontHtml = `
                        <div class="id-card template-${emp.template || 'classic'}" style="margin: 0; padding: 0; --theme-primary: ${emp.themePrimary}; --theme-secondary: ${emp.themeSecondary};">
                            <div class="bg-pattern"></div>
                            <div class="id-header z-10 relative" style="background: linear-gradient(90deg, ${emp.themePrimary} 0%, ${emp.themeSecondary} 100%);">
                                <div class="flex items-center justify-center gap-2 mb-1">
                                    <img src="${emp.logoSrc || ''}" class="absolute left-4 top-1/2 -translate-y-1/2 h-12 w-12 object-contain ${emp.logoSrc ? '' : 'hidden'}" alt="Logo">
                                    <h2 class="text-xl font-bold uppercase tracking-wider m-0 leading-tight">${emp.companyName}</h2>
                                </div>
                                <p class="text-xs opacity-80 m-0">Identity Card</p>
                            </div>
                            <div class="id-body z-10">
                                <div class="photo-container">
                                    <img src="${emp.photoSrc}" alt="Employee Photo" style="object-position: calc(50% + ${emp.photoOffsetX}px) calc(50% + ${emp.photoOffsetY}px);">
                                </div>
                                <h3 class="employee-name">${emp.name}</h3>
                                <p class="employee-title">${emp.designation}</p>
                                <div class="details-grid">
                                    <div class="detail-label">ID NO:</div>
                                    <div class="detail-value">${emp.empId}</div>
                                    <div class="detail-label">BLOOD:</div>
                                    <div class="detail-value text-red-600 font-bold">${emp.bloodGroup}</div>
                                    <div class="detail-label">PHONE:</div>
                                    <div class="detail-value">${emp.phone}</div>
                                    <div class="detail-label">D.O.B:</div>
                                    <div class="detail-value">${emp.dob}</div>
                                    <div class="detail-label">D.O.J:</div>
                                    <div class="detail-value">${emp.validTill}</div>
                                </div>
                            </div>
                            <div class="id-footer z-10 flex justify-center py-1">
                                <img src="${emp.barcodeSrc}" alt="Barcode" class="h-8 w-auto max-w-full object-contain mix-blend-multiply">
                            </div>
                        </div>
                    `;

                    const backHtml = `
                        <div class="id-card template-${emp.template || 'classic'}" style="margin: 0; padding: 0; --theme-primary: ${emp.themePrimary}; --theme-secondary: ${emp.themeSecondary};">
                            <div class="bg-pattern"></div>
                            <div class="id-header z-10 relative" style="height: 60px; background: linear-gradient(90deg, ${emp.themePrimary} 0%, ${emp.themeSecondary} 100%);">
                                <div class="flex items-center justify-center gap-2">
                                    <img src="${emp.logoSrc || ''}" class="absolute left-4 top-1/2 -translate-y-1/2 h-10 w-10 object-contain ${emp.logoSrc ? '' : 'hidden'}" alt="Logo">
                                    <h2 class="text-lg font-bold uppercase tracking-wider m-0 leading-tight">${emp.companyName}</h2>
                                </div>
                            </div>
                            <div class="id-body z-10 flex flex-col justify-center text-center px-4 w-full h-full">
                                <div class="mb-4">
                                    <h4 class="font-bold text-gray-800 text-sm mb-1 uppercase tracking-wide">Company Address</h4>
                                    <p class="text-xs text-gray-600 whitespace-pre-wrap leading-relaxed">${emp.address}</p>
                                </div>
                                <div class="mb-4">
                                    <h4 class="font-bold text-gray-800 text-sm mb-1 uppercase tracking-wide">Website</h4>
                                    <p class="text-xs text-gray-600">${emp.website}</p>
                                </div>
                                <div class="w-28 h-28 bg-white border border-gray-200 shadow-sm mx-auto flex items-center justify-center rounded-lg mb-2 p-1">
                                    <img src="${emp.qrCodeSrc}" alt="QR Code" class="w-full h-full object-contain">
                                </div>
                                <p class="text-[10px] text-gray-400 mt-2">Scan for verification</p>
                                <div class="mt-2 h-10 flex items-center justify-center">
                                    <img src="${emp.signSrc || ''}" alt="MD Signature" class="h-full object-contain ${emp.signSrc ? '' : 'hidden'}">
                                </div>
                            </div>
                            <div class="id-footer z-10 border-t border-gray-200 bg-gray-50">
                                <p class="text-[10px] text-gray-500 whitespace-pre-wrap leading-tight">${emp.returnText}</p>
                            </div>
                        </div>
                    `;

                    cardsList.push(createPrintableCard(frontHtml));
                    cardsList.push(createPrintableCard(backHtml));
                });
                
                const compName = inputs.companyName.value.trim() || 'Company';
                downloadPdfFromArea(compName, cardsList);
            });

            document.getElementById('printPdfBtn').addEventListener('click', () => {
                const frontClone = document.getElementById('idCardPreview').cloneNode(true);
                const backClone = document.getElementById('idCardPreviewBack').cloneNode(true);
                
                const card1 = createPrintableCard(frontClone.outerHTML);
                const card2 = createPrintableCard(backClone.outerHTML);
                
                renderPrintPages([card1, card2]);
                
                setTimeout(() => window.print(), 100);
            });

            document.getElementById('printBatchBtn').addEventListener('click', () => {
                if (employeeBatch.length === 0) return;
                
                const cardsList = [];
                employeeBatch.forEach((emp) => {
                    const frontHtml = `
                        <div class="id-card template-${emp.template || 'classic'}" style="margin: 0; padding: 0; --theme-primary: ${emp.themePrimary}; --theme-secondary: ${emp.themeSecondary};">
                            <div class="bg-pattern"></div>
                            <div class="id-header z-10 relative" style="background: linear-gradient(90deg, ${emp.themePrimary} 0%, ${emp.themeSecondary} 100%);">
                                <div class="flex items-center justify-center gap-2 mb-1">
                                    <img src="${emp.logoSrc || ''}" class="absolute left-4 top-1/2 -translate-y-1/2 h-12 w-12 object-contain ${emp.logoSrc ? '' : 'hidden'}" alt="Logo">
                                    <h2 class="text-xl font-bold uppercase tracking-wider m-0 leading-tight">${emp.companyName}</h2>
                                </div>
                                <p class="text-xs opacity-80 m-0">Identity Card</p>
                            </div>
                            <div class="id-body z-10">
                                <div class="photo-container">
                                    <img src="${emp.photoSrc}" alt="Employee Photo" style="object-position: calc(50% + ${emp.photoOffsetX}px) calc(50% + ${emp.photoOffsetY}px);">
                                </div>
                                <h3 class="employee-name">${emp.name}</h3>
                                <p class="employee-title">${emp.designation}</p>
                                <div class="details-grid">
                                    <div class="detail-label">ID NO:</div>
                                    <div class="detail-value">${emp.empId}</div>
                                    <div class="detail-label">BLOOD:</div>
                                    <div class="detail-value text-red-600 font-bold">${emp.bloodGroup}</div>
                                    <div class="detail-label">PHONE:</div>
                                    <div class="detail-value">${emp.phone}</div>
                                    <div class="detail-label">D.O.B:</div>
                                    <div class="detail-value">${emp.dob}</div>
                                    <div class="detail-label">D.O.J:</div>
                                    <div class="detail-value">${emp.validTill}</div>
                                </div>
                            </div>
                            <div class="id-footer z-10 flex justify-center py-1">
                                <img src="${emp.barcodeSrc}" alt="Barcode" class="h-8 w-auto max-w-full object-contain mix-blend-multiply">
                            </div>
                        </div>
                    `;

                    const backHtml = `
                        <div class="id-card template-${emp.template || 'classic'}" style="margin: 0; padding: 0; --theme-primary: ${emp.themePrimary}; --theme-secondary: ${emp.themeSecondary};">
                            <div class="bg-pattern"></div>
                            <div class="id-header z-10 relative" style="height: 60px; background: linear-gradient(90deg, ${emp.themePrimary} 0%, ${emp.themeSecondary} 100%);">
                                <div class="flex items-center justify-center gap-2">
                                    <img src="${emp.logoSrc || ''}" class="absolute left-4 top-1/2 -translate-y-1/2 h-10 w-10 object-contain ${emp.logoSrc ? '' : 'hidden'}" alt="Logo">
                                    <h2 class="text-lg font-bold uppercase tracking-wider m-0 leading-tight">${emp.companyName}</h2>
                                </div>
                            </div>
                            <div class="id-body z-10 flex flex-col justify-center text-center px-4 w-full h-full">
                                <div class="mb-4">
                                    <h4 class="font-bold text-gray-800 text-sm mb-1 uppercase tracking-wide">Company Address</h4>
                                    <p class="text-xs text-gray-600 whitespace-pre-wrap leading-relaxed">${emp.address}</p>
                                </div>
                                <div class="mb-4">
                                    <h4 class="font-bold text-gray-800 text-sm mb-1 uppercase tracking-wide">Website</h4>
                                    <p class="text-xs text-gray-600">${emp.website}</p>
                                </div>
                                <div class="w-28 h-28 bg-white border border-gray-200 shadow-sm mx-auto flex items-center justify-center rounded-lg mb-2 p-1">
                                    <img src="${emp.qrCodeSrc}" alt="QR Code" class="w-full h-full object-contain">
                                </div>
                                <p class="text-[10px] text-gray-400 mt-2">Scan for verification</p>
                                <div class="mt-2 h-10 flex items-center justify-center">
                                    <img src="${emp.signSrc || ''}" alt="MD Signature" class="h-full object-contain ${emp.signSrc ? '' : 'hidden'}">
                                </div>
                            </div>
                            <div class="id-footer z-10 border-t border-gray-200 bg-gray-50">
                                <p class="text-[10px] text-gray-500 whitespace-pre-wrap leading-tight">${emp.returnText}</p>
                            </div>
                        </div>
                    `;

                    cardsList.push(createPrintableCard(frontHtml));
                    cardsList.push(createPrintableCard(backHtml));
                });
                
                renderPrintPages(cardsList);
                
                setTimeout(() => window.print(), 100);
            });

            // Backup and Restore Logic
            document.getElementById('exportBackupBtn').addEventListener('click', () => {
                if (employeeBatch.length === 0) {
                    alert('Batch queue is empty. Nothing to export.');
                    return;
                }
                const dataStr = "data:text/json;charset=utf-8," + encodeURIComponent(JSON.stringify(employeeBatch));
                const downloadAnchorNode = document.createElement('a');
                downloadAnchorNode.setAttribute("href", dataStr);
                downloadAnchorNode.setAttribute("download", `id_card_batch_backup_${new Date().toISOString().slice(0,10)}.json`);
                document.body.appendChild(downloadAnchorNode); 
                downloadAnchorNode.click();
                downloadAnchorNode.remove();
            });

            document.getElementById('importBackupFile').addEventListener('change', (event) => {
                const file = event.target.files[0];
                if (!file) return;
                
                const reader = new FileReader();
                reader.onload = function(e) {
                    try {
                        const importedData = JSON.parse(e.target.result);
                        if (Array.isArray(importedData)) {
                            // Append to existing queue
                            employeeBatch = employeeBatch.concat(importedData);
                            updateBatchUI();
                            alert(`Successfully imported ${importedData.length} employees to the batch.`);
                        } else {
                            alert("Invalid backup file format. Expected a JSON array.");
                        }
                    } catch (err) {
                        alert("Error reading backup file. Make sure it is a valid JSON file.");
                    }
                };
                reader.readAsText(file);
                // Reset input so the same file can be imported again
                event.target.value = '';
            });

            // --- BULK EXCEL IMPORT & PHOTO MATCHING LOGIC ---
            const downloadTemplateBtn = document.getElementById('downloadExcelTemplateBtn');
            const bulkExcelUpload = document.getElementById('bulkExcelUpload');
            const bulkPhotoUpload = document.getElementById('bulkPhotoUpload');
            const importBulkBtn = document.getElementById('importBulkBtn');
            const bulkExcelFileInfo = document.getElementById('bulkExcelFileInfo');
            const bulkPhotoFileInfo = document.getElementById('bulkPhotoFileInfo');
            
            let bulkPhotosData = {}; // maps ID -> base64 DataURL

            // Dynamic Sample Template Generator and Downloader using SheetJS
            downloadTemplateBtn.addEventListener('click', () => {
                const sampleData = [
                    {
                        "Company Name": "R.S.D. Fashions",
                        "Full Name": "Jane Smith",
                        "Designation": "Marketing Manager",
                        "Employee ID": "EMP-2026-101",
                        "Blood Group": "B+",
                        "Phone": "9967715150",
                        "Date of Birth": "15/08/1992",
                        "Date of Joining": "01/05/2026",
                        "Company Address": "Nos. 300/2A, Thanneerpandhal Colony, Velampalayam, Tirupur - 641652.",
                        "Website": "rsdfashions.com",
                        "Return Policy": "If found, please return to R.S.D. Fashions HQ. This card is property of the issuing company.",
                        "Photo URL": "https://images.unsplash.com/photo-1494790108377-be9c29b29330?w=150"
                    },
                    {
                        "Company Name": "R.S.D. Fashions",
                        "Full Name": "Alex Johnson",
                        "Designation": "Senior Analyst",
                        "Employee ID": "EMP-2026-102",
                        "Blood Group": "O-",
                        "Phone": "8682861656",
                        "Date of Birth": "22/11/1988",
                        "Date of Joining": "12/04/2026",
                        "Company Address": "Nos. 300/2A, Thanneerpandhal Colony, Velampalayam, Tirupur - 641652.",
                        "Website": "rsdfashions.com",
                        "Return Policy": "If found, please return to R.S.D. Fashions HQ. This card is property of the issuing company.",
                        "Photo URL": "https://images.unsplash.com/photo-1507003211169-0a1dd7228f2d?w=150"
                    }
                ];
                
                const ws = XLSX.utils.json_to_sheet(sampleData);
                const wb = XLSX.utils.book_new();
                XLSX.utils.book_append_sheet(wb, ws, "Employees");
                XLSX.writeFile(wb, "id_card_bulk_template.xlsx");
            });

            // Display Excel File Info
            bulkExcelUpload.addEventListener('change', (e) => {
                const file = e.target.files[0];
                if (file) {
                    bulkExcelFileInfo.textContent = `📄 Selected: ${file.name} (${(file.size / 1024).toFixed(1)} KB)`;
                    bulkExcelFileInfo.classList.remove('hidden');
                } else {
                    bulkExcelFileInfo.classList.add('hidden');
                }
            });

            // Multi-Photo selector and ID matching base64 converter
            bulkPhotoUpload.addEventListener('change', (e) => {
                const files = e.target.files;
                bulkPhotosData = {};
                
                if (files.length > 0) {
                    bulkPhotoFileInfo.textContent = `⏳ Reading ${files.length} photo(s)...`;
                    bulkPhotoFileInfo.classList.remove('hidden');
                    
                    let loadedCount = 0;
                    for (let file of files) {
                        if (!file.type.startsWith('image/')) continue;
                        
                        const idx = file.name.lastIndexOf('.');
                        const empId = (idx !== -1 ? file.name.substring(0, idx) : file.name).trim().toUpperCase();
                        
                        const reader = new FileReader();
                        reader.onload = function(evt) {
                            bulkPhotosData[empId] = evt.target.result;
                            loadedCount++;
                            if (loadedCount === files.length) {
                                bulkPhotoFileInfo.textContent = `📸 Ready: Loaded ${loadedCount} employee photo(s)`;
                            }
                        };
                        reader.readAsDataURL(file);
                    }
                } else {
                    bulkPhotoFileInfo.classList.add('hidden');
                }
            });

            // Dedicated bulk logo and signature upload handlers
            const bulkLogoUpload = document.getElementById('bulkLogoUpload');
            const bulkLogoUploadText = document.getElementById('bulkLogoUploadText');
            const bulkSignUpload = document.getElementById('bulkSignUpload');
            const bulkSignUploadText = document.getElementById('bulkSignUploadText');

            if (bulkLogoUpload) {
                bulkLogoUpload.addEventListener('change', (e) => {
                    const file = e.target.files[0];
                    if (file) {
                        const reader = new FileReader();
                        reader.onload = function(evt) {
                            bulkLogoSrc = evt.target.result;
                            bulkLogoUploadText.textContent = `✅ Selected: ${file.name}`;
                            bulkLogoUploadText.className = "font-semibold text-green-600";
                        };
                        reader.readAsDataURL(file);
                    } else {
                        bulkLogoSrc = null;
                        bulkLogoUploadText.textContent = "Apply logo to all imported cards";
                        bulkLogoUploadText.className = "text-[10px] text-slate-400 font-normal";
                    }
                });
            }

            if (bulkSignUpload) {
                bulkSignUpload.addEventListener('change', (e) => {
                    const file = e.target.files[0];
                    if (file) {
                        const reader = new FileReader();
                        reader.onload = function(evt) {
                            bulkSignSrc = evt.target.result;
                            bulkSignUploadText.textContent = `✅ Selected: ${file.name}`;
                            bulkSignUploadText.className = "font-semibold text-green-600";
                        };
                        reader.readAsDataURL(file);
                    } else {
                        bulkSignSrc = null;
                        bulkSignUploadText.textContent = "Apply signature to all imported cards";
                        bulkSignUploadText.className = "text-[10px] text-slate-400 font-normal";
                    }
                });
            }

            // Parse spreadsheet & import batch data
            importBulkBtn.addEventListener('click', () => {
                const file = bulkExcelUpload.files[0];
                if (!file) {
                    alert("Please select an Excel or CSV file first.");
                    return;
                }
                
                const originalText = importBulkBtn.innerHTML;
                importBulkBtn.disabled = true;
                importBulkBtn.innerHTML = `<svg class="animate-spin w-4 h-4 mr-1.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8H4z"></path></svg> Importing...`;
                
                const reader = new FileReader();
                reader.onload = function(evt) {
                    try {
                        const data = evt.target.result;
                        const workbook = XLSX.read(data, { type: 'binary' });
                        const sheetName = workbook.SheetNames[0];
                        const sheet = workbook.Sheets[sheetName];
                        const rows = XLSX.utils.sheet_to_json(sheet);
                        
                        if (rows.length === 0) {
                            alert("The spreadsheet appears to be empty.");
                            resetImportButton();
                            return;
                        }
                        
                        const previousBatchBackup = [...employeeBatch];
                        let importedCount = 0;
                        let errorLog = [];
                        
                        rows.forEach((row, index) => {
                            const rowNum = index + 2;
                            
                            // Headers mapping support
                            const companyName = row['Company Name'] || row['Company'] || inputs.companyName.value || 'Company Name';
                            const name = row['Full Name'] || row['Name'] || '';
                            const designation = row['Designation'] || row['Role'] || '';
                            const empId = (row['Employee ID'] || row['ID'] || row['Emp ID'] || '').toString().trim().toUpperCase();
                            const bloodGroup = (row['Blood Group'] || row['Blood'] || '').toString().trim().toUpperCase();
                            const phone = (row['Phone'] || row['Phone Number'] || row['Mobile'] || '').toString().replace(/\D/g, '').substring(0, 10);
                            const dob = formatAndCorrectDate((row['Date of Birth'] || row['DOB'] || '').toString());
                            const validTill = formatAndCorrectDate((row['Date of Joining'] || row['DOJ'] || '').toString());
                            const address = row['Company Address'] || row['Address'] || inputs.address.value || 'Company Address';
                            const website = row['Website'] || row['Company Website'] || inputs.website.value || 'Website';
                            const returnText = row['Return Policy'] || row['Footer Text'] || inputs.returnText.value || '';
                            
                            // Validations
                            if (!name) errorLog.push(`Row ${rowNum}: 'Full Name' is missing.`);
                            if (!designation) errorLog.push(`Row ${rowNum}: 'Designation' is missing.`);
                            if (!empId) errorLog.push(`Row ${rowNum}: 'Employee ID' is missing.`);
                            if (!bloodGroup) errorLog.push(`Row ${rowNum}: 'Blood Group' is missing.`);
                            if (phone.length < 10) errorLog.push(`Row ${rowNum}: 'Phone' must be a valid 10-digit number.`);
                            if (dob.length < 10) errorLog.push(`Row ${rowNum}: 'Date of Birth' must be in DD/MM/YYYY format.`);
                            if (!validTill) errorLog.push(`Row ${rowNum}: 'Date of Joining' is missing.`);
                            
                            if (errorLog.length > 0) return;
                            
                            // Photo assignment (Matched ID > URL > default avatar placeholder)
                            let photoSrc = bulkPhotosData[empId] || row['Photo URL'] || "data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCIgZmlsbD0iIzljYTNhaiI+PHBhdGggZD0iTTEyIDEyYzIuMjEgMCA0LTEuNzkgNC00cy0xLjc5LTQtNC00LTQgMS43OS00IDQgMS43OSA0IDQgNHptMCAyYy0yLjY3IDAtOCAxLjM0LTggNHYyaDE2di0yYzAtMi42Ni01LjMzLTQtOC00eiIvPjwvc3ZnPg==";
                            
                            // In-memory Barcode pre-generation
                            let barcodeSrc = "";
                            if (window.JsBarcode) {
                                const canvas = document.createElement('canvas');
                                try {
                                    JsBarcode(canvas, empId, {
                                        format: "CODE128",
                                        displayValue: false,
                                        height: 30,
                                        width: 1.5,
                                        margin: 0
                                    });
                                    barcodeSrc = canvas.toDataURL();
                                } catch (e) {}
                            }
                            
                            // In-memory QR code pre-generation
                            const qrData = `Name: ${name}\nID: ${empId}\nDOB: ${dob}\nRole: ${designation}\nPhone: ${phone}\nBlood Group: ${bloodGroup}`;
                            const qr = new QRious({
                                value: qrData,
                                size: 256,
                                level: 'M'
                            });
                            const qrCodeSrc = qr.toDataURL();
                            
                            const cardData = {
                                batchId: Date.now().toString() + '_' + index,
                                companyName: companyName,
                                themePrimary: inputs.themePrimary.value,
                                themeSecondary: inputs.themeSecondary.value,
                                name: name,
                                designation: designation,
                                empId: empId,
                                bloodGroup: bloodGroup,
                                phone: phone,
                                dob: dob,
                                validTill: validTill,
                                address: address,
                                website: website,
                                returnText: returnText,
                                photoSrc: photoSrc,
                                photoOffsetX: 0,
                                photoOffsetY: 0,
                                qrCodeSrc: qrCodeSrc,
                                barcodeSrc: barcodeSrc,
                                logoSrc: bulkLogoSrc || currentLogoSrc,
                                signSrc: bulkSignSrc || currentSignSrc,
                                template: inputs.selectedTemplate.value || 'classic'
                            };
                             
                             // Check for duplicates by Employee ID
                             const existingIndex = employeeBatch.findIndex(e => e.empId.toUpperCase() === empId.toUpperCase());
                             if (existingIndex !== -1) {
                                 // Keep the original batchId to preserve UI list key mapping references
                                 cardData.batchId = employeeBatch[existingIndex].batchId;
                                 employeeBatch[existingIndex] = cardData;
                             } else {
                                 employeeBatch.push(cardData);
                             }
                             importedCount++;
                         });
                         
                         if (errorLog.length > 0) {
                             alert(`Import Failed!\n\nPlease fix the following errors in your spreadsheet before importing again:\n\n${errorLog.slice(0, 5).join('\n')}\n${errorLog.length > 5 ? `...and ${errorLog.length - 5} more error(s).` : ''}`);
                             
                             // Restore backup array to prevent partial imports
                             employeeBatch = previousBatchBackup;
                         } else {
                            updateBatchUI();
                            
                            // Auto-preview the first card in the batch queue
                            if (employeeBatch.length > 0) {
                                window.editFromBatch(employeeBatch[0].batchId);
                            }
                            
                            importBulkBtn.innerHTML = `<svg class="w-4 h-4 mr-1.5" fill="none" stroke="currentColor" stroke-width="2.5" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7"></path></svg> Success! Imported ${importedCount} Employees`;
                            importBulkBtn.className = "w-full bg-green-600 hover:bg-green-700 text-white font-semibold py-3 px-4 rounded-xl transition duration-150 flex justify-center items-center text-xs shadow-md shadow-green-100 hover:shadow-lg active:scale-[0.98] transform";
                            
                            // Reset file selectors UI
                            bulkExcelUpload.value = '';
                            bulkPhotoUpload.value = '';
                            if (bulkLogoUpload) bulkLogoUpload.value = '';
                            if (bulkSignUpload) bulkSignUpload.value = '';
                            bulkExcelFileInfo.classList.add('hidden');
                            bulkPhotoFileInfo.classList.add('hidden');
                            if (bulkLogoUploadText) {
                                bulkLogoUploadText.textContent = "Apply logo to all imported cards";
                                bulkLogoUploadText.className = "text-[10px] text-slate-400 font-normal";
                            }
                            if (bulkSignUploadText) {
                                bulkSignUploadText.textContent = "Apply signature to all imported cards";
                                bulkSignUploadText.className = "text-[10px] text-slate-400 font-normal";
                            }
                            bulkPhotosData = {};
                            bulkLogoSrc = null;
                            bulkSignSrc = null;
                            
                            setTimeout(() => {
                                importBulkBtn.innerHTML = originalText;
                                importBulkBtn.className = "w-full bg-indigo-600 hover:bg-indigo-700 text-white font-semibold py-3 px-4 rounded-xl transition duration-150 flex justify-center items-center text-xs shadow-md shadow-indigo-100 hover:shadow-lg active:scale-[0.98] transform";
                            }, 3000);
                        }
                        
                    } catch (err) {
                        alert("Error reading Excel spreadsheet: Make sure your file is valid and follows the template format.");
                    }
                    
                    importBulkBtn.disabled = false;
                };
                
                reader.onerror = function() {
                    alert("Failed to read the file.");
                    resetImportButton();
                };
                
                function resetImportButton() {
                    importBulkBtn.disabled = false;
                    importBulkBtn.innerHTML = originalText;
                }
                
                reader.readAsBinaryString(file);
            });

            // --- Accordion Logic System ---
            
            // Helper function to expand a section programmatically
            function expandSection(targetId) {
                const targetContent = document.getElementById(targetId);
                const header = document.querySelector(`[data-target="${targetId}"]`);
                if (targetContent && header) {
                    const chevron = header.querySelector('svg');
                    targetContent.classList.remove('hidden');
                    if (chevron) {
                        chevron.classList.add('rotate-90');
                    }
                }
            }

            // Accordion click listeners
            const accordionHeaders = document.querySelectorAll('.accordion-header');
            accordionHeaders.forEach(header => {
                header.addEventListener('click', () => {
                    const targetId = header.getAttribute('data-target');
                    const targetContent = document.getElementById(targetId);
                    const chevron = header.querySelector('svg');
                    
                    if (targetContent) {
                        targetContent.classList.toggle('hidden');
                    }
                    if (chevron) {
                        chevron.classList.toggle('rotate-90');
                    }
                });
            });

            // Export helper for edit flow integration
            window.expandSection = expandSection;

            // Initial render
            updatePreview();
        });
    