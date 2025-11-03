import path from 'node:path';
import { promisify } from 'node:util';
import { execFile } from 'node:child_process';
import { readdir, mkdir, rename, stat, unlink } from 'node:fs/promises';
import fs from 'node:fs';

const execFileAsync = promisify(execFile);

const BASE_DIR = path.resolve('web/saledocs/noticeofsale');

const MONTH_NAMES = [
  'January', 'February', 'March', 'April', 'May', 'June',
  'July', 'August', 'September', 'October', 'November', 'December'
];
const MONTH_PATTERN = MONTH_NAMES.join('|');
const MONTH_INDICES = MONTH_NAMES.reduce((acc, name, index) => {
  acc[name.toLowerCase()] = index;
  return acc;
}, {});

const TIME_PATTERN = '(?<time>\\d{1,2}:\\d{2}\\s*(?:[ap]\\.?m\\.?)?)';

const ORDINAL_PATTERN = '\\d{1,2}(?:st|nd|rd|th)?';

const MONTH_NAME_DATE_REGEX = new RegExp(
  `(?:the\\s+(?<leadingDay>${ORDINAL_PATTERN})\\s+day\\s+of\\s+)?` + // optional “the 26th day of”
  `(?<month>${MONTH_PATTERN})` +
  `(?:\\s+(?<trailingDay>${ORDINAL_PATTERN}))?` +                   // keeps handling “September 5”
  `\\s*,\\s*(?<year>\\d{4}),?` +
  `\\s+at\\s*(?:${TIME_PATTERN}|Room)`,
  'i'
);

const NUMERIC_DATE_REGEX = new RegExp(
  `(?:on\\s+)?(?<monthNum>\\d{1,2})[\\/](?<dayNum>\\d{1,2})[\\/](?<year>\\d{4})` +
  `\\s+at\\s*${TIME_PATTERN}`,
  'i'
);

const DATE_PATTERNS = [
  { type: 'monthName', regex: MONTH_NAME_DATE_REGEX },
  { type: 'numeric', regex: NUMERIC_DATE_REGEX },
];


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

function stripOrdinalSuffix(value) {
  return value ? value.replace(/(st|nd|rd|th)$/i, '') : value;
}

function parseTimeComponents(raw) {
  if (!raw) {
    return null;
  }
  const cleaned = raw.replace(/\./g, '').replace(/\s+/g, '').toUpperCase();
  const timeMatch = cleaned.match(/^(\d{1,2}):(\d{2})(AM|PM)?$/);
  if (!timeMatch) {
    return null;
  }

  let hour = parseInt(timeMatch[1], 10);
  const minute = parseInt(timeMatch[2], 10);
  const meridiem = timeMatch[3];

  if (Number.isNaN(hour) || Number.isNaN(minute) || minute > 59) {
    return null;
  }

  if (meridiem) {
    if (hour === 12) {
      hour = meridiem === 'AM' ? 0 : 12;
    } else if (meridiem === 'PM') {
      hour += 12;
    }
  } else if (hour >= 24) {
    return null;
  }

  return { hour, minute };
}

function parseDateFromMatch(match, type) {
  if (!match?.groups) {
    return null;
  }

  let day;
  let monthIndex;
  let parsedYear;

  if (type === 'monthName') {
    const { leadingDay, trailingDay, month, year } = match.groups;
    if (DEBUG) { console.log(`${leadingDay}, ${trailingDay}, ${month}, ${year}`); }
    const dayToken = trailingDay ?? leadingDay;
    if (!dayToken) {
      return null;
    }
    day = parseInt(stripOrdinalSuffix(dayToken), 10);
    if (!Number.isInteger(day) || day < 1 || day > 31) {
      return null;
    }

    monthIndex = MONTH_INDICES[month.toLowerCase()];
    if (monthIndex === undefined) {
      return null;
    }

    parsedYear = parseInt(year, 10);
  } else if (type === 'numeric') {
    const { monthNum, dayNum, year } = match.groups;
    if (DEBUG) { console.log(`${monthNum}, ${dayNum}, ${year}`); }
    monthIndex = parseInt(monthNum, 10) - 1;
    day = parseInt(dayNum, 10);
    parsedYear = parseInt(year, 10);

    if (!Number.isInteger(monthIndex) || monthIndex < 0 || monthIndex > 11) {
      return null;
    }
    if (!Number.isInteger(day) || day < 1 || day > 31) {
      return null;
    }
  } else {
    return null;
  }

  if (!Number.isInteger(parsedYear)) {
    return null;
  }

  let timeComponents = parseTimeComponents(match.groups.time);
  if (!timeComponents) {
    timeComponents = { hour: 14, minute: 30 };
  }

  const date = new Date(
    parsedYear,
    monthIndex,
    day,
    timeComponents.hour,
    timeComponents.minute,
    0,
    0
  );

  return Number.isNaN(date.getTime()) ? null : date;
}

function formatDateDirectory(date) {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) {
    return null;
  }
  return date.toISOString().split('T')[0];
}

async function ensureDirectory(dirPath) {
  await mkdir(dirPath, { recursive: true });
}

const DEBUG = false;
const DEBUG_FILES = [
  'web/saledocs/noticeofsale/4028-2013.pdf',
];

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
  if (DEBUG) {
    console.log(normalizedText);
  }

  let match = null;
  let matchedPattern = null;
  for (const pattern of DATE_PATTERNS) {
    match = pattern.regex.exec(normalizedText);
    if (match) {
      matchedPattern = pattern;
      break;
    }
  }

  if (!match || !matchedPattern) {
    return { relativePath: path.relative(BASE_DIR, filePath), status: 'skipped', reason: 'no_date_with_time' };
  }

  if (DEBUG) {
    console.log(`Matched date: ${match[0]}`);
  }

  const parsedDate = parseDateFromMatch(match, matchedPattern.type);
  if (!parsedDate) {
    return { relativePath: path.relative(BASE_DIR, filePath), status: 'skipped', reason: 'invalid_date_components' };
  }

  const dateDirectory = formatDateDirectory(parsedDate);
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
    unlink(filePath)
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
  const pdfFiles = DEBUG ? DEBUG_FILES : await collectPdfFiles(BASE_DIR);

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
