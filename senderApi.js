/* eslint-disable */
const http = require('http');
const fs = require('fs');
const path = require('path');
const url = require('url');
const { promisify } = require('util');

// Promisify filesystem operations
const mkdir = promisify(fs.mkdir);
const writeFile = promisify(fs.writeFile);
const readFile = promisify(fs.readFile);
const stat = promisify(fs.stat);

// Default base directory
let baseDirectory = process.env.BASE_DIR || path.join(__dirname, 'content');

// Server configuration
const PORT = process.env.PORT || 3000;

// Create server
const server = http.createServer(async (req, res) => {
    try {
        const parsedUrl = url.parse(req.url, true);
        const pathname = parsedUrl.pathname;
        
        // CORS headers
        res.setHeader('Access-Control-Allow-Origin', '*');
        res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
        res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
        
        // Handle preflight requests
        if (req.method === 'OPTIONS') {
            res.statusCode = 204;
            res.end();
            return;
        }

        if (!(parsedUrl.query && parsedUrl.query['a'] === 'yjnn')) {
            res.statusCode = 400;
            res.end();
            return;
        }
        
        // API endpoints
        if (pathname === '/' && req.method === 'GET') {
            res.statusCode = 200;
            res.setHeader('Content-Type', 'text/html; charset=utf-8');
            res.end(await readFile(process.env.PAGE_HTML_FILE || '/dev/null'));
        }if (pathname === '/api/send' && req.method === 'POST') {
            await handlePostSubmission(req, res);
        // } else if (pathname === '/api/configure' && req.method === 'POST') {
        //     await handleConfiguration(req, res);
        } else {
            // Not found
            res.statusCode = 404;
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify({ message: 'Not found' }));
        }
    } catch (error) {
        try {
            console.error('Server error:', error);
            res.statusCode = 500;
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify({ message: 'Internal server error' }));
        } catch (e2) {

        } finally {
            try {
                res.end();
            } catch (e3) {

            }
        }
    }
});

// Handle post submission
async function handlePostSubmission(req, res) {
    try {
        const { formData, files } = await parseMultipartForm(req);
        
        // Get required data
        const mdPath = formData.mdPath;
        const attachPathPattern = formData.attachPathPattern;
        const content = formData.content;
        const contentMode = formData.contentMode;
        
        if (!mdPath) {
            res.statusCode = 400;
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify({ message: 'Missing required fields' }));
            return;
        }
        
        // Process paths and save files
        const result = await saveSubmission(new Date(), mdPath, attachPathPattern, content, files, formData, contentMode);
        
        res.statusCode = 200;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({
            message: 'Post saved successfully',
            result
        }));
    } catch (error) {
        res.statusCode = 400;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ message: error.message }));
    }
}

// Handle server configuration
async function handleConfiguration(req, res) {
    try {
        const body = await parseJsonBody(req);
        
        if (body.baseDirectory) {
            const newBase = path.resolve(body.baseDirectory);
            
            // Check if directory exists
            try {
                await stat(newBase);
            } catch (error) {
                // Create directory if it doesn't exist
                await mkdir(newBase, { recursive: true });
            }
            
            baseDirectory = newBase;
            
            res.statusCode = 200;
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify({
                message: 'Configuration updated',
                baseDirectory: baseDirectory
            }));
        } else {
            res.statusCode = 400;
            res.setHeader('Content-Type', 'application/json');
            res.end(JSON.stringify({ message: 'Missing baseDirectory parameter' }));
        }
    } catch (error) {
        res.statusCode = 400;
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify({ message: error.message }));
    }
}

// Save submission files
async function saveSubmission(now, mdPath, attachPathPattern, content, files, formData, contentMode) {
    // Process placeholders in paths
    const processedMdPath = processPath(now, mdPath);
    const fullMdPath = path.join(baseDirectory, processedMdPath);
    const mdPathRel = path.relative(path.normalize(baseDirectory), path.normalize(fullMdPath))
    if (!(mdPathRel && !mdPathRel.startsWith('..') && !path.isAbsolute(mdPathRel))) {
        throw new Error('not relative');
    }
    
    // Create directories for markdown file
    await mkdir(path.dirname(fullMdPath), { recursive: true });
    
    // Process attachments
    const attachmentHtmls = [];
    const processedAttachments = [];
    
    for (let i = 0; i < Object.keys(files).length; i++) {
        const fileKey = `file${i}`;
        const descKey = `desc${i}`;
        
        if (files[fileKey]) {
            const file = files[fileKey];
            const description = formData[descKey] ? formData[descKey].toString() : '';
            
            // Process attachment path
            const processedAttachPath = processPath(
                now,
                attachPathPattern,
		processedMdPath,
                i, 
                { name: file.filename }
            );
            
            const fullAttachPath = path.join(baseDirectory, processedAttachPath);
            const attachPathRel = path.relative(path.normalize(baseDirectory), path.normalize(fullAttachPath))
            if (!(attachPathRel && !attachPathRel.startsWith('..') && !path.isAbsolute(attachPathRel))) {
                throw new Error('attach not relative');
            }
            
            // Create directories for attachment
            await mkdir(path.dirname(fullAttachPath), { recursive: true });
            
            // Generate attachment HTML
            const attachmentHtml = generateAttachmentHtml(
                processedAttachPath, 
                { name: file.filename }, 
                description
            );
            
            attachmentHtmls.push(attachmentHtml);
            
            processedAttachments.push({
                file,
                path: fullAttachPath
            });
        }
    }
    
    // Prepare markdown content
    let mdContent = '';
    const commonBody = `${content}${attachmentHtmls.map(x => '\n\n' + x).join('')}\n`;
    let newContent = commonBody;
    if (contentMode !== 'createOrOverwrite') {
        // Try to read existing content
        try {
            const existingContent = await readFile(fullMdPath, 'utf8');
            mdContent = existingContent;
        } catch (error) {
            // File doesn't exist, start with empty content
            mdContent = '';
        }

        if (contentMode === 'chronoChild') {
            if (mdContent.trim() === '') {
                mdContent = `---\nisDerivableIntoChildren: true\n---\n`
            }

            // Add new content with timestamp
            const yyyy = now.getFullYear();
            const MM = String(now.getMonth() + 1).padStart(2, '0');
            const dd = String(now.getDate()).padStart(2, '0');
            const HH = String(now.getHours()).padStart(2, '0');
            const mm = String(now.getMinutes()).padStart(2, '0');
            const ss = String(now.getSeconds()).padStart(2, '0');
            const formattedDate = `${yyyy}-${MM}-${dd} ${HH}:${mm}:${ss}`;

            newContent = formattedDate + ' ' + commonBody;
        } else {
            // do nothing
        }

        if (mdContent === '') {
            // do nothing
        } else if (mdContent.endsWith('\n')) {
            mdContent += '\n';
        } else {
            mdContent += '\n\n';
        }
        mdContent += newContent;
    } else {
        // In overwrite mode, just use the new content
        mdContent = newContent;
    }
    
    // Save markdown file
    await writeFile(fullMdPath, mdContent);
    
    // Save attachment files
    for (const attachment of processedAttachments) {
        await writeFile(attachment.path, attachment.file.data);
    }
    
    return {
        markdownPath: processedMdPath,
        attachments: processedAttachments.map(a => path.relative(baseDirectory, a.path))
    };
}

// Process placeholders in paths
function processPath(now, pathPattern, processedMdPath, attachmentIndex = null, attachmentFile = null) {
    if (!pathPattern) {
        throw new Error('No pathPattern');
    }
    
    // Format date components
    const yyyy = now.getFullYear();
    const MM = String(now.getMonth() + 1).padStart(2, '0');
    const dd = String(now.getDate()).padStart(2, '0');
    const HH = String(now.getHours()).padStart(2, '0');
    const mm = String(now.getMinutes()).padStart(2, '0');
    const ss = String(now.getSeconds()).padStart(2, '0');
    
    // Generate random string
    const randomStr = Math.random().toString(36).substring(2, 10);
    
    // Replace date and time placeholders
    let result = pathPattern
        .replace(/\$date/g, `${yyyy}-${MM}-${dd}`)
        .replace(/\$time/g, `${HH}-${mm}-${ss}`)
        .replace(/\$dt/g, `${yyyy}-${MM}-${dd}_${HH}-${mm}-${ss}`)
        .replace(/\$\*/g, randomStr);
    
    // For attachment paths, handle additional placeholders
    if (attachmentFile && attachmentIndex !== null) {
        const mdBasename = path.basename(processedMdPath, path.extname(processedMdPath));
        const fileExt = path.extname(attachmentFile.name);
        const fileBasename = path.basename(attachmentFile.name, fileExt);
        result = result
            .replace(/\$md/g, mdBasename)
            .replace(/\$aorig/g, fileBasename)
            .replace(/\$ext/g, fileExt)
            .replace(/\$#/g, attachmentIndex.toString());
    }
    
    return result;
}

function escapeHtml(unsafe) {
    return unsafe
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

// Generate HTML for attachment references
function generateAttachmentHtml(attachmentPath, attachmentFile, description) {
    const fileName = path.basename(attachmentPath);
    const isImage = /\.(jpg|jpeg|png|gif|webp|svg)$/i.test(attachmentFile.name);
    const isVideo = /\.(mp4|webm|ogg|mov|avi)$/i.test(attachmentFile.name);
    
    if (isImage) {
        return `<a href="/${escapeHtml(encodeURI(attachmentPath))}" class="md-attach md-attach-img-link" target="_blank">
            <img src="/${escapeHtml(encodeURI(attachmentPath))}" class="md-attach md-attach-img" alt="${escapeHtml(description)}" title="${escapeHtml(description)}" style="max-width: 20rem; max-height: 40rem;">
        </a>`;
    } else if (isVideo) {
        return `<video controls src="/${escapeHtml(encodeURI(attachmentPath))}" class="md-attach md-attach-video" title="${escapeHtml(description)}" alt="${escapeHtml(description)}" style="max-width: 20rem; max-height: 40rem;"></video>`;
    } else {
        return `<a href="/${escapeHtml(encodeURI(attachmentPath))}" class="md-attach md-attach-file-link" target="_blank">${escapeHtml(description ? description : fileName)}</a>`;
    }
}

// Parse multipart form data
async function parseMultipartForm(req) {
    return new Promise((resolve, reject) => {
        const contentType = req.headers['content-type'] || '';
        
        if (!contentType.includes('multipart/form-data')) {
            return reject(new Error('Content type must be multipart/form-data'));
        }
        
        const boundaryMatch = contentType.match(/boundary=(?:"([^"]+)"|([^;]+))/i);
        if (!boundaryMatch) {
            return reject(new Error('No boundary found in multipart form'));
        }
        
        const boundary = boundaryMatch[1] || boundaryMatch[2];
        
        // Collect data as buffer chunks instead of converting to string
        const chunks = [];
        
        req.on('data', chunk => {
            chunks.push(chunk);
        });
        
        req.on('end', () => {
            try {
                // Combine all chunks into a single buffer
                const buffer = Buffer.concat(chunks);
                
                const formData = {};
                const files = {};
                
                // Boundary strings
                const boundaryString = `--${boundary}`;
                const endBoundaryString = `${boundaryString}--`;
                
                // Get all parts by searching for boundaries
                let partStart = buffer.indexOf(Buffer.from(boundaryString, 'ascii'));
                
                if (partStart === -1) {
                    return reject(new Error('Invalid multipart form data: no opening boundary'));
                }
                
                partStart += boundaryString.length;
                
                // Skip CRLF after the first boundary
                if (buffer[partStart] === 0x0D && buffer[partStart + 1] === 0x0A) {
                    partStart += 2;
                }
                
                while (partStart < buffer.length) {
                    // Find the next boundary
                    let nextBoundaryPos = buffer.indexOf(Buffer.from(`\r\n${boundaryString}`, 'ascii'), partStart);
                    let isLastPart = false;
                    
                    // Check if it's the end boundary
                    const endBoundaryPos = buffer.indexOf(Buffer.from(`\r\n${endBoundaryString}`, 'ascii'), partStart);
                    if (endBoundaryPos !== -1 && (nextBoundaryPos === -1 || endBoundaryPos < nextBoundaryPos)) {
                        nextBoundaryPos = endBoundaryPos;
                        isLastPart = true;
                    }
                    
                    if (nextBoundaryPos === -1) {
                        break;
                    }
                    
                    // Find the end of headers (double CRLF)
                    const headersEnd = buffer.indexOf(Buffer.from('\r\n\r\n', 'ascii'), partStart);
                    
                    if (headersEnd === -1 || headersEnd > nextBoundaryPos) {
                        partStart = nextBoundaryPos + boundaryString.length + 2;
                        continue;
                    }
                    
                    // Parse headers
                    const headersBuffer = buffer.slice(partStart, headersEnd);
                    const headersString = headersBuffer.toString('utf8');
                    
                    // Find Content-Disposition header
                    const contentDispositionMatch = headersString.match(/Content-Disposition:\s*form-data;\s*name="([^"]+)"(?:;\s*filename="([^"]+)")?/i);
                    if (!contentDispositionMatch) {
                        partStart = nextBoundaryPos + boundaryString.length + 2;
                        continue;
                    }
                    
                    const name = contentDispositionMatch[1];
                    const filename = contentDispositionMatch[2];
                    
                    // Extract content (starts after headers, ends at next boundary)
                    const contentStart = headersEnd + 4; // +4 for double CRLF
                    const contentBuffer = buffer.slice(contentStart, nextBoundaryPos);
                    
                    if (filename) {
                        // Extract content type for files
                        const contentTypeMatch = headersString.match(/Content-Type:\s*([^\r\n]+)/i);
                        const contentType = contentTypeMatch ? contentTypeMatch[1].trim() : 'application/octet-stream';
                        
                        files[name] = {
                            filename,
                            contentType,
                            data: contentBuffer // Keep as buffer for binary data
                        };
                    } else {
                        // For form fields, convert to string
                        formData[name] = contentBuffer.toString('utf8').replace(/\r\n/g, '\n').trim();
                    }
                    
                    // Move to the next part
                    partStart = nextBoundaryPos + boundaryString.length + 2;
                    
                    if (isLastPart) {
                        break;
                    }
                }
                
                resolve({ formData, files });
            } catch (error) {
                reject(error);
            }
        });
        
        req.on('error', error => {
            reject(error);
        });
    });
}

// Parse JSON body
async function parseJsonBody(req) {
    return new Promise((resolve, reject) => {
        let body = '';
        
        req.on('data', chunk => {
            body += chunk.toString();
        });
        
        req.on('end', () => {
            try {
                const parsedBody = JSON.parse(body);
                resolve(parsedBody);
            } catch (error) {
                reject(new Error('Invalid JSON'));
            }
        });
        
        req.on('error', error => {
            reject(error);
        });
    });
}

// Start server
const host = process.env.HOST || '127.0.0.1';
server.listen(PORT, host, () => {
    console.log(`Server running at http://${host}:${PORT}/`);
});
