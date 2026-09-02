document.addEventListener('DOMContentLoaded', () => {
    // DOM Elements
    const step1 = document.getElementById('step-1');
    const step2 = document.getElementById('step-2');
    const step3 = document.getElementById('step-3');
    
    const uploadDropzone = document.getElementById('uploadDropzone');
    const imageInput = document.getElementById('imageInput');
    const cropperImage = document.getElementById('cropperImage');
    
    const photoSizeSelect = document.getElementById('photoSize');
    const paperSizeSelect = document.getElementById('paperSize');
    
    const cancelCropBtn = document.getElementById('cancelCropBtn');
    const generateBtn = document.getElementById('generateBtn');
    
    const resultCanvas = document.getElementById('resultCanvas');
    const downloadJpgBtn = document.getElementById('downloadJpgBtn');
    const downloadPdfBtn = document.getElementById('downloadPdfBtn');
    const printBtn = document.getElementById('printBtn');
    const startOverBtn = document.getElementById('startOverBtn');

    let cropper = null;
    
    // Photo dimensions in mm
    const photoDims = {
        '51x51': { w: 51, h: 51 },
        '35x45': { w: 35, h: 45 },
        '33x48': { w: 33, h: 48 }
    };
    
    // Paper dimensions in mm
    const paperDims = {
        'A4': { w: 210, h: 297 },
        '4x6': { w: 102, h: 152 },
        '5x7': { w: 127, h: 178 }
    };

    // DPI for rendering (300 DPI is standard for print)
    const DPI = 300;
    const MM_TO_INCH = 25.4;
    
    function mmToPx(mm) {
        return Math.round((mm / MM_TO_INCH) * DPI);
    }

    // Drag and Drop
    uploadDropzone.addEventListener('click', () => imageInput.click());

    const handleDrag = (e) => {
        e.preventDefault();
        e.stopPropagation();
        if (e.type === 'dragenter' || e.type === 'dragover') {
            uploadDropzone.classList.add('dragover');
        } else {
            uploadDropzone.classList.remove('dragover');
        }
    };
    
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(evt => {
        uploadDropzone.addEventListener(evt, handleDrag);
    });

    uploadDropzone.addEventListener('drop', (e) => {
        if(e.dataTransfer.files.length > 0) handleFile(e.dataTransfer.files[0]);
    });

    imageInput.addEventListener('change', (e) => {
        if(e.target.files.length > 0) handleFile(e.target.files[0]);
    });

    function handleFile(file) {
        if (file && file.type.startsWith('image/')) {
            const reader = new FileReader();
            reader.onload = (e) => {
                cropperImage.src = e.target.result;
                initCropper();
                step1.classList.remove('block');
                step1.classList.add('hidden');
                step2.classList.remove('hidden');
                step2.classList.add('block');
            };
            reader.readAsDataURL(file);
        } else {
            alert('Please select a valid image file (JPG/PNG).');
        }
    }

    function initCropper() {
        if (cropper) {
            cropper.destroy();
        }
        const sizeKey = photoSizeSelect.value;
        const dim = photoDims[sizeKey] || photoDims['35x45'];
        const aspectRatio = dim.w / dim.h;
        
        cropper = new Cropper(cropperImage, {
            aspectRatio: aspectRatio,
            viewMode: 1,
            dragMode: 'move',
            autoCropArea: 0.8,
            restore: false,
            guides: true,
            center: true,
            highlight: false,
            cropBoxMovable: true,
            cropBoxResizable: true,
            toggleDragModeOnDblclick: false,
        });
    }

    cancelCropBtn.addEventListener('click', () => {
        if (cropper) cropper.destroy();
        imageInput.value = '';
        step2.classList.remove('block');
        step2.classList.add('hidden');
        step1.classList.remove('hidden');
        step1.classList.add('block');
    });

    let croppedCanvasData = null;

    function renderPassportSheet() {
        if (!croppedCanvasData) return;
        
        // Get photo size in px
        const sizeKey = photoSizeSelect.value;
        const pDim = photoDims[sizeKey] || photoDims['35x45'];
        const pWidthPx = mmToPx(pDim.w);
        const pHeightPx = mmToPx(pDim.h);
        
        // Get paper size in px
        const paperKey = paperSizeSelect.value;
        const pageDim = paperDims[paperKey] || paperDims['4x6'];
        
        const pageWidthPx = mmToPx(pageDim.w);
        const pageHeightPx = mmToPx(pageDim.h);
        
        // Create canvas
        resultCanvas.width = pageWidthPx;
        resultCanvas.height = pageHeightPx;
        const ctx = resultCanvas.getContext('2d');
        
        // Fill white background
        ctx.fillStyle = '#ffffff';
        ctx.fillRect(0, 0, pageWidthPx, pageHeightPx);
        
        // Margins and Spacing in px (e.g., 5mm)
        const gap = mmToPx(5);
        const marginX = mmToPx(5);
        const marginY = mmToPx(5);
        
        // Calculate columns and rows
        const cols = Math.floor((pageWidthPx - marginX * 2 + gap) / (pWidthPx + gap));
        const rows = Math.floor((pageHeightPx - marginY * 2 + gap) / (pHeightPx + gap));
        
        // Center the grid on the page
        const gridWidth = cols * pWidthPx + (cols - 1) * gap;
        const gridHeight = rows * pHeightPx + (rows - 1) * gap;
        
        const startX = (pageWidthPx - gridWidth) / 2;
        const startY = (pageHeightPx - gridHeight) / 2;
        
        // Background and Border settings
        const bgColor = document.getElementById('photoBgColor') ? document.getElementById('photoBgColor').value : 'transparent';
        const borderColor = document.getElementById('photoBorderColor') ? document.getElementById('photoBorderColor').value : 'none';
        const borderWidthPx = borderColor === 'none' ? 0 : Math.max(1, mmToPx(0.5)); // Standard border width
        
        // Draw images
        for (let r = 0; r < rows; r++) {
            for (let c = 0; c < cols; c++) {
                const x = startX + c * (pWidthPx + gap);
                const y = startY + r * (pHeightPx + gap);
                
                // Fill background color if selected
                if (bgColor !== 'transparent') {
                    ctx.fillStyle = bgColor;
                    ctx.fillRect(x, y, pWidthPx, pHeightPx);
                }
                
                ctx.drawImage(croppedCanvasData, x, y, pWidthPx, pHeightPx);
                
                // Draw border or cutting marks
                if (borderColor === 'cropmarks') {
                    ctx.strokeStyle = '#64748b'; // Slate 500 for a nice subtle grey cutting mark
                    ctx.lineWidth = Math.max(1, mmToPx(0.25));
                    const l = mmToPx(2.5); // length of the crop mark line
                    
                    ctx.beginPath();
                    // Top-Left
                    ctx.moveTo(x, y); ctx.lineTo(x - l, y);
                    ctx.moveTo(x, y); ctx.lineTo(x, y - l);
                    
                    // Top-Right
                    ctx.moveTo(x + pWidthPx, y); ctx.lineTo(x + pWidthPx + l, y);
                    ctx.moveTo(x + pWidthPx, y); ctx.lineTo(x + pWidthPx, y - l);
                    
                    // Bottom-Right
                    ctx.moveTo(x + pWidthPx, y + pHeightPx); ctx.lineTo(x + pWidthPx + l, y + pHeightPx);
                    ctx.moveTo(x + pWidthPx, y + pHeightPx); ctx.lineTo(x + pWidthPx, y + pHeightPx + l);
                    
                    // Bottom-Left
                    ctx.moveTo(x, y + pHeightPx); ctx.lineTo(x - l, y + pHeightPx);
                    ctx.moveTo(x, y + pHeightPx); ctx.lineTo(x, y + pHeightPx + l);
                    
                    ctx.stroke();
                } else if (borderColor !== 'none') {
                    ctx.strokeStyle = borderColor;
                    ctx.lineWidth = borderWidthPx;
                    ctx.strokeRect(x, y, pWidthPx, pHeightPx);
                }
            }
        }
    }

    generateBtn.addEventListener('click', async () => {
        if (!cropper) return;
        
        generateBtn.disabled = true;
        const originalText = generateBtn.innerHTML;
        generateBtn.innerHTML = '<span class="animate-pulse">Processing...</span>';

        try {
            const sizeKey = photoSizeSelect.value;
            const pDim = photoDims[sizeKey] || photoDims['35x45'];
            const pWidthPx = mmToPx(pDim.w);
            const pHeightPx = mmToPx(pDim.h);
            
            // Crop Image
            let canvas = cropper.getCroppedCanvas({
                width: pWidthPx,
                height: pHeightPx,
                imageSmoothingEnabled: true,
                imageSmoothingQuality: 'high',
            });
            
            const removeBgToggle = document.getElementById('removeBgToggle');
            if (removeBgToggle && removeBgToggle.checked) {
                generateBtn.innerHTML = `
                    <div class="flex flex-col items-center w-full px-2">
                        <span class="animate-pulse">Removing Background (AI)...</span>
                        <span class="text-xs font-normal opacity-80 mt-1">First run downloads AI model (can take a few secs). Please wait...</span>
                        <div class="w-full bg-white/30 rounded-full h-1.5 mt-2 overflow-hidden">
                            <div id="aiProgressBar" class="bg-white h-1.5 rounded-full transition-all duration-300" style="width: 0%"></div>
                        </div>
                    </div>
                `;
                let progress = 0;
                const progressBar = document.getElementById('aiProgressBar');
                const progressInterval = setInterval(() => {
                    if (progress < 90) {
                        progress += Math.random() * 10;
                        if (progress > 90) progress = 90;
                        if (progressBar) progressBar.style.width = progress + '%';
                    }
                }, 400);
                const blob = await new Promise(resolve => canvas.toBlob(resolve, 'image/jpeg', 0.95));
                const formData = new FormData();
                formData.append('file', blob, 'image.jpg');
                
                const response = await fetch('/api/remove-background', {
                    method: 'POST',
                    body: formData
                });
                
                clearInterval(progressInterval);
                if (progressBar && response.ok) progressBar.style.width = '100%';
                
                if (!response.ok) {
                    clearInterval(progressInterval);
                    let errorText = 'Background removal failed';
                    try {
                        const errJson = await response.json();
                        if (errJson.error) errorText = errJson.error;
                    } catch (e) {
                        errorText += ' (Status: ' + response.status + ')';
                    }
                    throw new Error(errorText);
                }
                
                const responseBlob = await response.blob();
                const img = new Image();
                img.src = URL.createObjectURL(responseBlob);
                await new Promise(resolve => img.onload = resolve);
                
                // Draw back onto a canvas of the exact same size
                const newCanvas = document.createElement('canvas');
                newCanvas.width = pWidthPx;
                newCanvas.height = pHeightPx;
                const newCtx = newCanvas.getContext('2d');
                newCtx.drawImage(img, 0, 0, pWidthPx, pHeightPx);
                canvas = newCanvas;
            }

            croppedCanvasData = canvas;
            renderPassportSheet();
            
            step2.classList.remove('block');
            step2.classList.add('hidden');
            step3.classList.remove('hidden');
            step3.classList.add('block');
        } catch (error) {
            alert("Error generating passport photo: " + error.message);
        } finally {
            generateBtn.disabled = false;
            generateBtn.innerHTML = originalText;
        }
    });

    // Add change listeners for live layout updating in Step 3
    [paperSizeSelect, document.getElementById('photoBgColor'), document.getElementById('photoBorderColor')].forEach(el => {
        if (el) {
            el.addEventListener('change', () => {
                if (step3.classList.contains('block')) {
                    renderPassportSheet();
                }
            });
        }
    });

    startOverBtn.addEventListener('click', () => {
        imageInput.value = '';
        step3.classList.remove('block');
        step3.classList.add('hidden');
        step1.classList.remove('hidden');
        step1.classList.add('block');
    });

    downloadJpgBtn.addEventListener('click', () => {
        const link = document.createElement('a');
        link.download = 'passport_photos.jpg';
        link.href = resultCanvas.toDataURL('image/jpeg', 1.0);
        link.click();
    });

    downloadPdfBtn.addEventListener('click', () => {
        if (!window.jspdf) {
            alert("PDF library is still loading, please wait.");
            return;
        }
        const { jsPDF } = window.jspdf;
        const paperKey = paperSizeSelect.value;
        const pageDim = paperDims[paperKey] || paperDims['4x6'];
        
        // jsPDF dimensions in mm
        const pdf = new jsPDF({
            orientation: 'portrait',
            unit: 'mm',
            format: [pageDim.w, pageDim.h]
        });
        
        const imgData = resultCanvas.toDataURL('image/jpeg', 1.0);
        pdf.addImage(imgData, 'JPEG', 0, 0, pageDim.w, pageDim.h);
        pdf.save('passport_photos.pdf');
    });

    printBtn.addEventListener('click', () => {
        const imgData = resultCanvas.toDataURL('image/jpeg', 1.0);
        const printWindow = window.open('', '_blank');
        if (printWindow) {
            printWindow.document.write(`
                <html>
                    <head>
                        <title>Print Passport Photos - MobileFix Pro</title>
                        <style>
                            @page { margin: 0; }
                            body { margin: 0; padding: 0; display: flex; justify-content: center; align-items: flex-start; background: #fff; }
                            img { max-width: 100%; height: auto; }
                        </style>
                    </head>
                    <body>
                        <img src="${imgData}" onload="window.print(); setTimeout(() => window.close(), 500);" />
                    </body>
                </html>
            `);
            printWindow.document.close();
        } else {
            alert('Please allow popups to print the photos.');
        }
    });

});
