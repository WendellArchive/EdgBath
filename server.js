const express = require('express');
const multer = require('multer');
const archiver = require('archiver');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

const app = express();
const PORT = process.env.PORT || 3000;

const UPLOADS_DIR = path.join(__dirname, 'uploads');
const ZIPS_DIR = path.join(__dirname, 'zips');
const TEMP_DIR = path.join(__dirname, 'temp');

for (const dir of [UPLOADS_DIR, ZIPS_DIR, TEMP_DIR]) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, UPLOADS_DIR),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname) || '';
    cb(null, Date.now() + '-' + crypto.randomBytes(4).toString('hex') + ext);
  }
});
const upload = multer({ storage });

const progressMap = new Map();
const CLEANUP_INTERVAL = 5 * 60 * 1000;
setInterval(() => {
  const now = Date.now();
  for (const [jobId, data] of progressMap) {
    if (now - data.created > 30 * 60 * 1000) {
      progressMap.delete(jobId);
    }
  }
}, CLEANUP_INTERVAL);

app.use(express.json());
app.use(express.static('public'));
app.use('/uploads', express.static(UPLOADS_DIR));
app.use('/zips', express.static(ZIPS_DIR));

app.post('/upload', upload.array('files'), (req, res) => {
  if (!req.files || req.files.length === 0) {
    return res.status(400).json({ error: 'No files uploaded' });
  }
  const baseUrl = `${req.protocol}://${req.get('host')}`;
  res.json({
    success: true,
    count: req.files.length,
    files: req.files.map(f => ({
      name: f.filename,
      originalName: f.originalname,
      url: baseUrl + '/uploads/' + f.filename
    }))
  });
});

app.get('/api/files', (req, res) => {
  fs.readdir(UPLOADS_DIR, (err, files) => {
    if (err) return res.status(500).json({ error: err.message });
    files.sort().reverse();
    const result = files.map(f => {
      const stat = fs.statSync(path.join(UPLOADS_DIR, f));
      return {
        name: f,
        size: stat.size,
        lastModified: stat.mtimeMs
      };
    });
    res.json(result);
  });
});

app.delete('/api/files', (req, res) => {
  const { files } = req.body;
  if (!files || !Array.isArray(files)) {
    return res.status(400).json({ error: 'No files specified' });
  }
  for (const f of files) {
    const fp = path.join(UPLOADS_DIR, f);
    if (fs.existsSync(fp) && !fs.lstatSync(fp).isDirectory()) {
      fs.unlinkSync(fp);
    }
  }
  res.json({ success: true });
});

app.post('/api/generate-zips', (req, res) => {
  let files;
  try {
    files = fs.readdirSync(UPLOADS_DIR);
    files = files.filter(f => fs.statSync(path.join(UPLOADS_DIR, f)).isFile());
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }

  if (files.length === 0) {
    return res.json({ zips: [], jobId: null, totalChunks: 0 });
  }

  const jobId = Date.now().toString() + '-' + crypto.randomBytes(4).toString('hex');
  const CHUNK_SIZE = 80;
  const chunks = [];
  for (let i = 0; i < files.length; i += CHUNK_SIZE) {
    chunks.push(files.slice(i, i + CHUNK_SIZE));
  }

  progressMap.set(jobId, {
    total: chunks.length,
    completed: 0,
    zips: [],
    error: null,
    created: Date.now(),
    chunks
  });

  generateZips(jobId).catch(err => {
    const p = progressMap.get(jobId);
    if (p) p.error = err.message;
  });

  res.json({ jobId, totalChunks: chunks.length, totalFiles: files.length });
});

async function generateZips(jobId) {
  const progress = progressMap.get(jobId);
  if (!progress) return;

  const { chunks } = progress;

  for (let i = 0; i < chunks.length; i++) {
    const p = progressMap.get(jobId);
    if (!p || p.error) break;

    const zipName = `photos_part_${i + 1}_of_${chunks.length}.zip`;
    const zipPath = path.join(ZIPS_DIR, zipName);

    try {
      await new Promise((resolve, reject) => {
        const output = fs.createWriteStream(zipPath);
        const archive = archiver('zip', { zlib: { level: 5 } });

        output.on('close', () => resolve());
        archive.on('error', (err) => reject(err));

        archive.pipe(output);

        for (const file of chunks[i]) {
          const filePath = path.join(UPLOADS_DIR, file);
          if (fs.existsSync(filePath)) {
            archive.file(filePath, { name: file });
          }
        }

        archive.finalize();
      });

      const p2 = progressMap.get(jobId);
      if (p2) {
        p2.completed++;
        p2.zips.push({ name: zipName, index: i + 1, total: chunks.length });
      }
    } catch (err) {
      const p3 = progressMap.get(jobId);
      if (p3) p3.error = err.message;
      break;
    }
  }
}

app.get('/api/zip-progress/:jobId', (req, res) => {
  const { jobId } = req.params;

  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'X-Accel-Buffering': 'no'
  });

  const sendProgress = () => {
    const progress = progressMap.get(jobId);
    if (!progress) {
      res.write(`event: error\ndata: {"error":"Job not found"}\n\n`);
      cleanup();
      return;
    }

    if (progress.error) {
      res.write(`event: error\ndata: ${JSON.stringify({ error: progress.error })}\n\n`);
      cleanup();
      return;
    }

    res.write(`data: ${JSON.stringify({ completed: progress.completed, total: progress.total })}\n\n`);

    if (progress.completed >= progress.total) {
      res.write(`event: done\ndata: ${JSON.stringify(progress.zips)}\n\n`);
      cleanup();
      return;
    }
  };

  const cleanup = () => {
    clearInterval(interval);
    res.end();
  };

  const interval = setInterval(sendProgress, 250);
  req.on('close', cleanup);
});

app.get('/api/download-zip', (req, res) => {
  const zipName = req.query.name;
  if (!zipName) return res.status(400).json({ error: 'Missing zip name' });

  const zipPath = path.join(ZIPS_DIR, path.basename(zipName));
  if (!fs.existsSync(zipPath)) {
    return res.status(404).json({ error: 'Zip not found' });
  }

  res.download(zipPath, zipName);
});

app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({ error: err.message || 'Internal server error' });
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
