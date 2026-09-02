document.addEventListener('DOMContentLoaded', () => {
    // --- Tabs Logic ---
    const tabBtns = document.querySelectorAll('.tab-btn');
    const tabContents = document.querySelectorAll('.tab-content');

    tabBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            tabBtns.forEach(b => b.classList.remove('active', 'border-b-2', 'border-teal-500'));
            tabContents.forEach(c => {
                c.classList.remove('block');
                c.classList.add('hidden');
            });

            btn.classList.add('active', 'border-b-2', 'border-teal-500');
            const target = btn.getAttribute('data-tab');
            document.getElementById('tab-' + target).classList.remove('hidden');
            document.getElementById('tab-' + target).classList.add('block');
        });
    });

    // Initialize first tab styles
    document.querySelector('.tab-btn.active').classList.add('border-b-2', 'border-teal-500');

    const overlay = document.getElementById('loadingOverlay');
    const showLoader = () => {
        overlay.classList.remove('hidden');
        overlay.classList.add('opacity-100', 'pointer-events-auto');
    };
    const hideLoader = () => {
        overlay.classList.add('hidden');
        overlay.classList.remove('opacity-100', 'pointer-events-auto');
    };

    // Helper: download blob
    
    const customAlert = document.getElementById('customAlert');
    const customAlertBox = document.getElementById('customAlertBox');
    const customAlertIcon = document.getElementById('customAlertIcon');
    const customAlertTitle = document.getElementById('customAlertTitle');
    const customAlertMessage = document.getElementById('customAlertMessage');
    const customAlertBtn = document.getElementById('customAlertBtn');

    window.showCustomAlert = (title, message, type = 'error') => {
        if (!customAlert) return alert(title + ": " + message);
        customAlertTitle.textContent = title;
        customAlertMessage.textContent = message;
        
        if (type === 'error') {
            customAlertIcon.className = 'w-16 h-16 rounded-full flex items-center justify-center mb-4 bg-red-50 text-red-500';
            customAlertIcon.innerHTML = '<svg class="w-8 h-8" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"></path></svg>';
        } else {
            customAlertIcon.className = 'w-16 h-16 rounded-full flex items-center justify-center mb-4 bg-teal-50 text-teal-500';
            customAlertIcon.innerHTML = '<svg class="w-8 h-8" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7"></path></svg>';
        }

        customAlert.classList.remove('hidden');
        setTimeout(() => {
            customAlert.classList.remove('opacity-0');
            customAlertBox.classList.remove('scale-95');
            customAlertBox.classList.add('scale-100');
        }, 10);
    };

    if (customAlertBtn) {
        customAlertBtn.addEventListener('click', () => {
            customAlert.classList.add('opacity-0');
            customAlertBox.classList.remove('scale-100');
            customAlertBox.classList.add('scale-95');
            setTimeout(() => {
                customAlert.classList.add('hidden');
            }, 300);
        });
    }


    const downloadBlob = (blob, filename) => {
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    };

    // --- Merge Tool ---
    let mergeFiles = [];
    const mergeInput = document.getElementById('mergeInput');
    const mergeDropzone = document.getElementById('mergeDropzone');
    const mergeFileList = document.getElementById('mergeFileList');
    const mergeBtn = document.getElementById('mergeBtn');

    mergeDropzone.addEventListener('click', () => mergeInput.click());
    
    const handleMergeDrag = (e) => {
        e.preventDefault();
        e.stopPropagation();
        if (e.type === 'dragenter' || e.type === 'dragover') {
            mergeDropzone.classList.add('dragover');
        } else {
            mergeDropzone.classList.remove('dragover');
        }
    };
    
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(evt => {
        mergeDropzone.addEventListener(evt, handleMergeDrag);
    });

    mergeDropzone.addEventListener('drop', (e) => {
        handleMergeFiles(e.dataTransfer.files);
    });

    mergeInput.addEventListener('change', (e) => {
        handleMergeFiles(e.target.files);
    });

    function handleMergeFiles(files) {
        for (let file of files) {
            if (file.type === 'application/pdf') {
                mergeFiles.push(file);
            }
        }
        renderMergeList();
    }

    function renderMergeList() {
        mergeFileList.innerHTML = '';
        mergeFiles.forEach((file, idx) => {
            const div = document.createElement('div');
            div.className = 'flex items-center justify-between bg-slate-50 border border-slate-200 rounded-lg p-3';
            div.innerHTML = `
                <div class="flex items-center gap-2 overflow-hidden">
                    <svg class="w-5 h-5 text-red-500 shrink-0" fill="currentColor" viewBox="0 0 20 20"><path d="M9 2a2 2 0 00-2 2v8a2 2 0 002 2h6a2 2 0 002-2V6.414A2 2 0 0016.414 5L14 2.586A2 2 0 0012.586 2H9z" /></svg>
                    <span class="text-sm font-medium text-slate-700 truncate">${file.name}</span>
                </div>
                <button type="button" class="text-slate-400 hover:text-red-500 transition-colors" onclick="removeMergeFile(${idx})">
                    <svg class="w-5 h-5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12"></path></svg>
                </button>
            `;
            mergeFileList.appendChild(div);
        });
        mergeBtn.disabled = mergeFiles.length < 2;
    }

    window.removeMergeFile = (idx) => {
        mergeFiles.splice(idx, 1);
        renderMergeList();
    };

    mergeBtn.addEventListener('click', async () => {
        if (mergeFiles.length < 2) return;
        showLoader();
        try {
            const mergedPdf = await PDFLib.PDFDocument.create();
            for (let file of mergeFiles) {
                const arrayBuffer = await file.arrayBuffer();
                const pdf = await PDFLib.PDFDocument.load(arrayBuffer);
                const copiedPages = await mergedPdf.copyPages(pdf, pdf.getPageIndices());
                copiedPages.forEach((page) => mergedPdf.addPage(page));
            }
            const pdfBytes = await mergedPdf.save();
            const blob = new Blob([pdfBytes], { type: 'application/pdf' });
            downloadBlob(blob, 'merged_document.pdf');
        } catch (e) {
            console.error(e);
            showCustomAlert('Merge Error', 'Error merging PDFs.', 'error');
        }
        hideLoader();
    });


    // --- Split Tool ---
    let splitFile = null;
    const splitInput = document.getElementById('splitInput');
    const splitDropzone = document.getElementById('splitDropzone');
    const splitFileName = document.getElementById('splitFileName');
    const splitBtn = document.getElementById('splitBtn');
    const splitPages = document.getElementById('splitPages');

    splitDropzone.addEventListener('click', () => splitInput.click());

    const handleSplitDrag = (e) => {
        e.preventDefault();
        e.stopPropagation();
        if (e.type === 'dragenter' || e.type === 'dragover') {
            splitDropzone.classList.add('dragover');
        } else {
            splitDropzone.classList.remove('dragover');
        }
    };
    
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(evt => {
        splitDropzone.addEventListener(evt, handleSplitDrag);
    });

    splitDropzone.addEventListener('drop', (e) => {
        if(e.dataTransfer.files.length > 0) handleSplitFile(e.dataTransfer.files[0]);
    });

    splitInput.addEventListener('change', (e) => {
        if(e.target.files.length > 0) handleSplitFile(e.target.files[0]);
    });

    function handleSplitFile(file) {
        if (file.type === 'application/pdf') {
            splitFile = file;
            splitFileName.textContent = file.name;
            splitFileName.classList.remove('hidden');
            splitBtn.disabled = false;
        }
    }

    splitBtn.addEventListener('click', async () => {
        if (!splitFile) return;
        const pageString = splitPages.value.trim();
        if (!pageString) {
            showCustomAlert('Input Required', 'Please enter pages to extract.', 'info');
            return;
        }
        
        showLoader();
        try {
            const arrayBuffer = await splitFile.arrayBuffer();
            const pdfDoc = await PDFLib.PDFDocument.load(arrayBuffer);
            const totalPages = pdfDoc.getPageCount();
            
            // Parse pages string (e.g. "1, 3, 5-8")
            let pagesToExtract = new Set();
            const parts = pageString.split(',');
            for (let part of parts) {
                part = part.trim();
                if (part.includes('-')) {
                    let [start, end] = part.split('-');
                    start = parseInt(start);
                    end = parseInt(end);
                    if (!isNaN(start) && !isNaN(end) && start > 0 && end <= totalPages) {
                        for(let i=start; i<=end; i++) pagesToExtract.add(i-1); // 0-indexed
                    }
                } else {
                    let val = parseInt(part);
                    if (!isNaN(val) && val > 0 && val <= totalPages) {
                        pagesToExtract.add(val-1);
                    }
                }
            }
            
            const indices = Array.from(pagesToExtract).sort((a,b) => a-b);
            if(indices.length === 0) {
                showCustomAlert('Invalid Selection', 'Invalid page selection or out of bounds.', 'error');
                hideLoader();
                return;
            }

            const newPdf = await PDFLib.PDFDocument.create();
            const copiedPages = await newPdf.copyPages(pdfDoc, indices);
            copiedPages.forEach((page) => newPdf.addPage(page));

            const pdfBytes = await newPdf.save();
            const blob = new Blob([pdfBytes], { type: 'application/pdf' });
            downloadBlob(blob, 'extracted_pages.pdf');
        } catch (e) {
            console.error(e);
            showCustomAlert('Split Error', 'Error splitting PDF.', 'error');
        }
        hideLoader();
    });


    // --- Images to PDF Tool ---
    let imgFiles = [];
    const imgInput = document.getElementById('imgInput');
    const imgDropzone = document.getElementById('imgDropzone');
    const imgFileList = document.getElementById('imgFileList');
    const imgBtn = document.getElementById('imgBtn');
    const imgPageSize = document.getElementById('imgPageSize');
    const imgOrientation = document.getElementById('imgOrientation');
    const imgMargin = document.getElementById('imgMargin');
    const imgOptionsBlock = document.getElementById('imgOptionsBlock');

    imgDropzone.addEventListener('click', () => imgInput.click());

    const handleImgDrag = (e) => {
        e.preventDefault();
        e.stopPropagation();
        if (e.type === 'dragenter' || e.type === 'dragover') {
            imgDropzone.classList.add('dragover');
        } else {
            imgDropzone.classList.remove('dragover');
        }
    };
    
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(evt => {
        imgDropzone.addEventListener(evt, handleImgDrag);
    });

    imgDropzone.addEventListener('drop', (e) => {
        handleImgFiles(e.dataTransfer.files);
    });

    imgInput.addEventListener('change', (e) => {
        handleImgFiles(e.target.files);
    });

    function handleImgFiles(files) {
        for (let file of files) {
            if (file.type.startsWith('image/')) {
                imgFiles.push(file);
            }
        }
        renderImgList();
    }

    function renderImgList() {
        imgFileList.innerHTML = '';
        imgFiles.forEach((file, idx) => {
            const div = document.createElement('div');
            div.className = 'relative group rounded-lg overflow-hidden border border-slate-200 aspect-square';
            
            const img = document.createElement('img');
            img.className = 'w-full h-full object-cover';
            img.src = URL.createObjectURL(file);
            
            const overlay = document.createElement('div');
            overlay.className = 'absolute inset-0 bg-black/40 opacity-0 group-hover:opacity-100 transition-opacity flex items-center justify-center';
            
            const btn = document.createElement('button');
            btn.className = 'bg-white text-red-500 rounded-full p-2 transform scale-75 group-hover:scale-100 transition-transform';
            btn.innerHTML = '<svg class="w-5 h-5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg>';
            btn.onclick = () => removeImgFile(idx);
            
            overlay.appendChild(btn);
            div.appendChild(img);
            div.appendChild(overlay);
            
            imgFileList.appendChild(div);
        });
        
        if (imgFiles.length > 0) {
            imgBtn.disabled = false;
            if (imgOptionsBlock) imgOptionsBlock.style.display = 'grid';
        } else {
            imgBtn.disabled = true;
            if (imgOptionsBlock) imgOptionsBlock.style.display = 'none';
        }
    }

    window.removeImgFile = (idx) => {
        imgFiles.splice(idx, 1);
        renderImgList();
    };

    imgBtn.addEventListener('click', async () => {
        if (imgFiles.length === 0) return;
        showLoader();
        try {
            const pdfDoc = await PDFLib.PDFDocument.create();
            
            for (let file of imgFiles) {
                const arrayBuffer = await file.arrayBuffer();
                let image;
                if (file.type === 'image/jpeg' || file.type === 'image/jpg') {
                    image = await pdfDoc.embedJpg(arrayBuffer);
                } else if (file.type === 'image/png') {
                    image = await pdfDoc.embedPng(arrayBuffer);
                } else {
                    // Ignore unsupported types
                    continue;
                }
                
                const dims = image.scale(1);
                const pageSizeStr = imgPageSize ? imgPageSize.value : 'fit';
                const orientStr = imgOrientation ? imgOrientation.value : 'auto';
                const marginVal = imgMargin ? parseInt(imgMargin.value) : 0;
                
                let pageWidth, pageHeight;
                let drawWidth, drawHeight;
                let drawX, drawY;

                if (pageSizeStr === 'A4') {
                    pageWidth = 595.28; pageHeight = 841.89;
                } else if (pageSizeStr === 'letter') {
                    pageWidth = 612.0; pageHeight = 792.0;
                }
                
                if (pageSizeStr !== 'fit') {
                    if (orientStr === 'landscape' || (orientStr === 'auto' && dims.width > dims.height)) {
                        [pageWidth, pageHeight] = [Math.max(pageWidth, pageHeight), Math.min(pageWidth, pageHeight)];
                    } else if (orientStr === 'portrait' || (orientStr === 'auto' && dims.width <= dims.height)) {
                        [pageWidth, pageHeight] = [Math.min(pageWidth, pageHeight), Math.max(pageWidth, pageHeight)];
                    }

                    const availWidth = pageWidth - (marginVal * 2);
                    const availHeight = pageHeight - (marginVal * 2);
                    const scaleFactor = Math.min(availWidth / dims.width, availHeight / dims.height);
                    drawWidth = dims.width * scaleFactor;
                    drawHeight = dims.height * scaleFactor;
                    drawX = marginVal + (availWidth - drawWidth) / 2;
                    drawY = marginVal + (availHeight - drawHeight) / 2;
                } else {
                    pageWidth = dims.width + (marginVal * 2);
                    pageHeight = dims.height + (marginVal * 2);
                    drawWidth = dims.width;
                    drawHeight = dims.height;
                    drawX = marginVal;
                    drawY = marginVal;
                }

                const page = pdfDoc.addPage([pageWidth, pageHeight]);
                page.drawImage(image, {
                    x: drawX,
                    y: drawY,
                    width: drawWidth,
                    height: drawHeight,
                });
            }
            
            const pdfBytes = await pdfDoc.save();
            const blob = new Blob([pdfBytes], { type: 'application/pdf' });
            downloadBlob(blob, 'images_to_pdf.pdf');
        } catch (e) {
            console.error(e);
            showCustomAlert('Conversion Error', 'Error converting images to PDF. Make sure they are JPG or PNG.', 'error');
        }
        hideLoader();
    });

    // --- PDF to Image Tool ---
    let pdf2imgFile = null;
    const pdf2imgInput = document.getElementById('pdf2imgInput');
    const pdf2imgDropzone = document.getElementById('pdf2imgDropzone');
    const pdf2imgFileName = document.getElementById('pdf2imgFileName');
    const pdf2imgBtn = document.getElementById('pdf2imgBtn');
    const pdf2imgFormat = document.getElementById('pdf2imgFormat');
    const pdf2imgPages = document.getElementById('pdf2imgPages');

    // Setup pdf.js worker
    if (window.pdfjsLib) {
        pdfjsLib.GlobalWorkerOptions.workerSrc = 'https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js';
    }

    pdf2imgDropzone.addEventListener('click', () => pdf2imgInput.click());

    const handlePdf2ImgDrag = (e) => {
        e.preventDefault();
        e.stopPropagation();
        if (e.type === 'dragenter' || e.type === 'dragover') {
            pdf2imgDropzone.classList.add('dragover');
        } else {
            pdf2imgDropzone.classList.remove('dragover');
        }
    };
    
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(evt => {
        pdf2imgDropzone.addEventListener(evt, handlePdf2ImgDrag);
    });

    pdf2imgDropzone.addEventListener('drop', (e) => {
        if(e.dataTransfer.files.length > 0) handlePdf2ImgFile(e.dataTransfer.files[0]);
    });

    pdf2imgInput.addEventListener('change', (e) => {
        if(e.target.files.length > 0) handlePdf2ImgFile(e.target.files[0]);
    });

    function handlePdf2ImgFile(file) {
        if (file.type === 'application/pdf') {
            pdf2imgFile = file;
            pdf2imgFileName.textContent = file.name;
            pdf2imgFileName.classList.remove('hidden');
            pdf2imgBtn.disabled = false;
        }
    }

    pdf2imgBtn.addEventListener('click', async () => {
        if (!pdf2imgFile) return;
        if (!window.pdfjsLib || !window.JSZip) {
            showCustomAlert('Loading', 'Libraries are still loading, please wait a second.', 'info');
            return;
        }
        showLoader();
        
        try {
            const format = pdf2imgFormat.value;
            const mimeType = format === 'png' ? 'image/png' : 'image/jpeg';
            const ext = format === 'png' ? 'png' : 'jpg';
            
            const arrayBuffer = await pdf2imgFile.arrayBuffer();
            const pdf = await pdfjsLib.getDocument({ data: arrayBuffer }).promise;
            
            const zip = new JSZip();
            
            const pageString = pdf2imgPages.value.trim();
            let pagesToConvert = [];
            const totalPages = pdf.numPages;
            if (pageString) {
                let pagesSet = new Set();
                const parts = pageString.split(',');
                for (let part of parts) {
                    part = part.trim();
                    if (part.includes('-')) {
                        let [start, end] = part.split('-');
                        start = parseInt(start);
                        end = parseInt(end);
                        if (!isNaN(start) && !isNaN(end) && start > 0 && end <= totalPages) {
                            for(let i=start; i<=end; i++) pagesSet.add(i);
                        }
                    } else {
                        let val = parseInt(part);
                        if (!isNaN(val) && val > 0 && val <= totalPages) {
                            pagesSet.add(val);
                        }
                    }
                }
                pagesToConvert = Array.from(pagesSet).sort((a,b) => a-b);
                if(pagesToConvert.length === 0) {
                    showCustomAlert('Invalid Selection', 'Invalid page selection or out of bounds.', 'error');
                    hideLoader();
                    return;
                }
            } else {
                for (let i = 1; i <= totalPages; i++) pagesToConvert.push(i);
            }
            
            for (let pageNum of pagesToConvert) {
                const page = await pdf.getPage(pageNum);
                const scale = 2.0; // High quality
                const viewport = page.getViewport({ scale: scale });
                
                const canvas = document.createElement('canvas');
                const ctx = canvas.getContext('2d');
                canvas.width = viewport.width;
                canvas.height = viewport.height;
                
                await page.render({
                    canvasContext: ctx,
                    viewport: viewport
                }).promise;
                
                const blob = await new Promise(resolve => canvas.toBlob(resolve, mimeType, 0.9));
                
                // Add to zip (e.g. "page-1.jpg")
                zip.file(`page-${pageNum}.${ext}`, blob);
            }
            
            const zipBlob = await zip.generateAsync({ type: 'blob' });
            downloadBlob(zipBlob, `${pdf2imgFile.name.replace('.pdf', '')}_images.zip`);
            
        } catch (e) {
            console.error(e);
            showCustomAlert('Conversion Error', 'Error converting PDF to images.', 'error');
        }
        
        hideLoader();
    });

    // --- Unlock PDF Tool ---
    let unlockFile = null;
    const unlockInput = document.getElementById('unlockInput');
    const unlockDropzone = document.getElementById('unlockDropzone');
    const unlockFileName = document.getElementById('unlockFileName');
    const unlockBtn = document.getElementById('unlockBtn');
    const unlockPassword = document.getElementById('unlockPassword');
    const togglePasswordBtn = document.getElementById('togglePasswordBtn');
    const eyeIconOpen = document.getElementById('eyeIconOpen');
    const eyeIconClosed = document.getElementById('eyeIconClosed');

    if (togglePasswordBtn) {
        togglePasswordBtn.addEventListener('click', () => {
            if (unlockPassword.type === 'password') {
                unlockPassword.type = 'text';
                eyeIconOpen.classList.remove('hidden');
                eyeIconOpen.classList.add('block');
                eyeIconClosed.classList.remove('block');
                eyeIconClosed.classList.add('hidden');
            } else {
                unlockPassword.type = 'password';
                eyeIconOpen.classList.remove('block');
                eyeIconOpen.classList.add('hidden');
                eyeIconClosed.classList.remove('hidden');
                eyeIconClosed.classList.add('block');
            }
        });
    }

    unlockDropzone.addEventListener('click', () => unlockInput.click());

    const handleUnlockDrag = (e) => {
        e.preventDefault();
        e.stopPropagation();
        if (e.type === 'dragenter' || e.type === 'dragover') {
            unlockDropzone.classList.add('dragover');
        } else {
            unlockDropzone.classList.remove('dragover');
        }
    };
    
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(evt => {
        unlockDropzone.addEventListener(evt, handleUnlockDrag);
    });

    unlockDropzone.addEventListener('drop', (e) => {
        if(e.dataTransfer.files.length > 0) handleUnlockFile(e.dataTransfer.files[0]);
    });

    unlockInput.addEventListener('change', (e) => {
        if(e.target.files.length > 0) handleUnlockFile(e.target.files[0]);
    });

    function handleUnlockFile(file) {
        if (file.type === 'application/pdf') {
            unlockFile = file;
            unlockFileName.textContent = file.name;
            unlockFileName.classList.remove('hidden');
            unlockBtn.disabled = false;
        }
    }

    unlockBtn.addEventListener('click', async () => {
        if (!unlockFile) return;
        const password = unlockPassword.value;
        if (!password) {
            showCustomAlert('Password Required', 'Please enter the original password to unlock this PDF.', 'info');
            return;
        }
        
        showLoader();
        try {
            const formData = new FormData();
            formData.append('file', unlockFile);
            formData.append('password', password);
            
            const response = await fetch('/api/unlock-pdf', {
                method: 'POST',
                body: formData
            });
            
            if (response.ok) {
                const blob = await response.blob();
                downloadBlob(blob, `${unlockFile.name.replace('.pdf', '')}_unlocked.pdf`);
                showCustomAlert('Success!', 'The PDF has been successfully unlocked and downloaded.', 'info');
                
                // Reset fields after successful operation
                unlockFile = null;
                unlockInput.value = '';
                unlockPassword.value = '';
                unlockFileName.textContent = '';
                unlockFileName.classList.add('hidden');
                // Optional: unlockBtn.disabled = true; (depending on if you initialize it as disabled)
            } else {
                let errorMsg = 'Incorrect password or unsupported encryption type.';
                try {
                    const errorData = await response.json();
                    if (errorData.message) errorMsg = errorData.message;
                } catch (e) {}
                showCustomAlert('Unlock Failed', errorMsg, 'error');
            }
        } catch (e) {
            console.error(e);
            showCustomAlert('Unlock Failed', 'Network error while contacting the server.', 'error');
        }
        hideLoader();
    });

});
