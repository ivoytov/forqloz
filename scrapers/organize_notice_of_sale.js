import path from 'node:path';
import { promisify } from 'node:util';
import { execFile } from 'node:child_process';
import { readdir, mkdir, rename, stat, unlink } from 'node:fs/promises';
import fs from 'node:fs';

const execFileAsync = promisify(execFile);

const BASE_DIR = path.resolve('web/saledocs/noticeofsale');

const MONTH_PATTERN = [
  'January', 'February', 'March', 'April', 'May', 'June',
  'July', 'August', 'September', 'October', 'November', 'December'
].join('|');

const TIME_PATTERN = '\\d{1,2}:\\d{2}\\s*(?:[ap]\\.?m\\.?)?';
const DATE_WITH_TIME_REGEX = new RegExp(
  `(${MONTH_PATTERN})\\s+\\d{1,2},\\s+\\d{4}(?=,?\\s+(at\s+)?\\s+${TIME_PATTERN})`,
  'i'
);

async function extractText(pdfPath) {
  try {
    const { stdout } = await execFileAsync('pdftotext', ['-layout', '-q', pdfPath, '-']);
    return stdout;
  } catch (error) {
    console.error(`Failed to extract text from ${pdfPath}: ${error.message}`);
    return null;
  }
}

function sanitizeWhitespace(value) {
  return value.replace(/\u00a0/g, ' ').replace(/\s+/g, ' ').trim();
}

function formatDateDirectory(rawDate) {
  const date = new Date(rawDate);
  if (Number.isNaN(date.getTime())) {
    return null;
  }
  return date.toISOString().split('T')[0];
}

async function ensureDirectory(dirPath) {
  await mkdir(dirPath, { recursive: true });
}

async function collectPdfFiles(dir) {
  const entries = await readdir(dir, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      // skip
      
    } else if (entry.isFile() && entry.name.toLowerCase().endsWith('.pdf')) {
      files.push(fullPath);
    }
  }
  return files;
}

async function processPdf(filePath) {
  const text = await extractText(filePath);
  if (!text) {
    return { relativePath: path.relative(BASE_DIR, filePath), status: 'failed', reason: 'text_extraction_failed' };
  }

  const normalizedText = sanitizeWhitespace(text);
  const match = normalizedText.match(DATE_WITH_TIME_REGEX);
  if (!match) {
    return { relativePath: path.relative(BASE_DIR, filePath), status: 'skipped', reason: 'no_date_with_time' };
  }

  const dateDirectory = formatDateDirectory(match[0]);
  if (!dateDirectory) {
    return { relativePath: path.relative(BASE_DIR, filePath), status: 'skipped', reason: 'invalid_date' };
  }

  const targetDir = path.join(BASE_DIR, dateDirectory);
  await ensureDirectory(targetDir);

  const fileName = path.basename(filePath);
  const targetPath = path.join(targetDir, fileName);
  if (path.resolve(filePath) === path.resolve(targetPath)) {
    return { relativePath: path.relative(BASE_DIR, filePath), status: 'already_sorted' };
  }

  if (fs.existsSync(targetPath)) {
    return { relativePath: path.relative(BASE_DIR, filePath), status: 'already_exists' };

  }

  await rename(filePath, targetPath);
  return {
    relativePath: path.relative(BASE_DIR, filePath),
    status: 'moved',
    destination: path.relative(BASE_DIR, targetPath),
  };
}

async function main() {
  const pdfFiles = await collectPdfFiles(BASE_DIR);

  if (pdfFiles.length === 0) {
    console.log('No PDF files found in the notice of sale directory.');
    return;
  }

  console.log(`Processing ${pdfFiles.length} PDF ${pdfFiles.length === 1 ? 'file' : 'files'}...`);

  const results = [];
  for (const pdfFile of pdfFiles) {
    const result = await processPdf(pdfFile);
    results.push(result);
  }

  for (const result of results) {
    if (result.status === 'moved') {
      console.log(`Moved ${result.relativePath} -> ${result.destination}`);
    } else if (result.status === 'removed_duplicate') {
      console.log(`Removed duplicate ${result.relativePath}; kept ${result.destination}`);
    } else {
      console.warn(`Skipped ${result.relativePath}: ${result.reason}`);
    }
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
