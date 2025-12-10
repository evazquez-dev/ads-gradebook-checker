const express = require('express');
const { google } = require('googleapis');
const { parse } = require('csv-parse/sync');

const app = express();

const CONFIG = {
  // ---- PowerSchool OAuth / PowerQuery ----
  psBaseUrl: process.env.PS_BASE_URL || 'https://americandreamschool.powerschool.com',
  psTokenUrl: process.env.PS_TOKEN_URL || 'https://americandreamschool.powerschool.com/oauth/access_token',
  psClientId: process.env.PS_CLIENT_ID,       // set these in Cloud Run env vars
  psClientSecret: process.env.PS_CLIENT_SECRET,
  pageSize: 5000,
  sleepMsBetweenPages: 150,
  scoreEntryDateFloor: '2025-08-01',          // same as GAS

  // query names from your plugin
  queries: {
    score_since: 'com.ads.assignments.score_since',
    section_by_score_since: 'com.ads.assignments.section_by_score_since'
  },

  // ---- CSV folder + names ----
  csvFolderName: '_PS_Assignments_Exports',
  csvNames: {
    scores:   'AssignmentScore_full.csv',
    sections: 'AssignmentSection_full.csv'
  }
};

// === CONFIG ===
const SPREADSHEET_ID = '1zPu05Vi6m_kt5PvF0n0caWFJf_P3FGs4qed18493UlA';
const EXPORT_FOLDER_NAME = '_PS_Assignments_Exports';
const SCORES_CSV_NAME = 'AssignmentScore_full.csv';
const SECTIONS_CSV_NAME = 'AssignmentSection_full.csv';

// Headers copied from your GAS file
const H_ASSIGNMENTSECTION = [
  'ASSIGNMENTID','AssignmentSectionID','DESCRIPTION','DUEDATE','EXECUTIONID',
  'EXTRACREDITPOINTS','IP_ADDRESS','ISCOUNTEDINFINALGRADE','ISSCORESPUBLISH','ISSCORINGNEEDED',
  'MAXRETAKEALLOWED','NAME','PUBLISHDAYSBEFOREDUE','PUBLISHEDDATE','PUBLISHEDSCORETYPEID',
  'PUBLISHONSPECIFICDATE','PUBLISHOPTION','RELATEDGRADESCALEITEMDCID','SCOREENTRYPOINTS','SCORETYPE',
  'SECTIONSDCID','TOTALPOINTVALUE','TRANSACTION_DATE','WEIGHT','WHOMODIFIEDID','WHOMODIFIEDTYPE','YEARID'
];

const H_ASSIGNMENTSCORE = [
  'ACTUALSCOREENTERED','ACTUALSCOREGRADESCALEDCID','ACTUALSCOREKIND','ALTALPHAGRADE','ALTNUMERICGRADE',
  'ALTSCOREGRADESCALEDCID','AssignmentScoreID','ASSIGNMENTSECTIONID','AUTHOREDBYUC','EXECUTIONID',
  'HASRETAKE','IP_ADDRESS','ISABSENT','ISCOLLECTED','ISEXEMPT','ISINCOMPLETE','ISLATE','ISMISSING',
  'SCOREENTRYDATE','SCOREGRADESCALEDCID','SCORELETTERGRADE','SCORENUMERICGRADE','SCOREPERCENT',
  'SCOREPOINTS','STUDENTSDCID','TRANSACTION_DATE','WHOMODIFIEDID','WHOMODIFIEDTYPE','YEARID'
];

// ==== Auth helpers ====

async function getAuthClient() {
  const auth = new google.auth.GoogleAuth({
    scopes: [
      'https://www.googleapis.com/auth/spreadsheets',
      'https://www.googleapis.com/auth/drive'
    ]
  });
  return auth.getClient();
}

function getSheets(auth) {
  return google.sheets({ version: 'v4', auth });
}

function getDrive(auth) {
  return google.drive({ version: 'v3', auth });
}

// PowerSchool helpers
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ---- PowerSchool auth (client_credentials) ----
let psToken = null;
let psTokenExpiry = 0;

async function getPsBearerToken() {
  if (psToken && Date.now() < psTokenExpiry - 60000) {
    return psToken;
  }

  if (!CONFIG.psClientId || !CONFIG.psClientSecret) {
    throw new Error('PS_CLIENT_ID or PS_CLIENT_SECRET env vars are not set');
  }

  const basic = Buffer.from(`${CONFIG.psClientId}:${CONFIG.psClientSecret}`).toString('base64');

  const res = await fetch(CONFIG.psTokenUrl, {
    method: 'POST',
    headers: {
      Authorization: `Basic ${basic}`,
      'Content-Type': 'application/x-www-form-urlencoded'
    },
    body: 'grant_type=client_credentials'
  });

  const text = await res.text();
  if (!res.ok) {
    throw new Error(`Token POST failed (${res.status}): ${text}`);
  }

  let json;
  try { json = JSON.parse(text); } catch (e) {
    throw new Error('Token response was not JSON: ' + text);
  }

  const token = json.access_token;
  const expiresIn = Math.max(60, Math.min(3600, json.expires_in || 3600));
  if (!token) throw new Error('Token response missing access_token: ' + text);

  psToken = token;
  psTokenExpiry = Date.now() + expiresIn * 1000;
  return token;
}

// 🔁 PowerSchool fetch with retries for transient socket/timeouts
async function psFetchJsonPost(url, bodyObj) {
  if (!CONFIG.psClientId || !CONFIG.psClientSecret) {
    throw new Error('Missing PS_CLIENT_ID or PS_CLIENT_SECRET env vars');
  }

  const maxAttempts = 5;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const basic = Buffer.from(`${CONFIG.psClientId}:${CONFIG.psClientSecret}`).toString('base64');

      // 1) Get bearer token
      const tokenResp = await fetch(CONFIG.psTokenUrl, {
        method: 'POST',
        headers: {
          Authorization: `Basic ${basic}`,
          'Content-Type': 'application/x-www-form-urlencoded'
        },
        body: 'grant_type=client_credentials'
      });

      const tokenText = await tokenResp.text();
      if (!tokenResp.ok) {
        throw new Error(`Token POST failed (${tokenResp.status}): ${tokenText}`);
      }

      let tokenJson;
      try {
        tokenJson = tokenText ? JSON.parse(tokenText) : {};
      } catch (e) {
        throw new Error(`Token JSON parse failed: ${e.message} — body=${tokenText}`);
      }

      const token = tokenJson.access_token;
      if (!token) {
        throw new Error('Token response missing access_token');
      }

      // 2) Call PowerQuery endpoint
      const resp = await fetch(url, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${token}`,
          Accept: 'application/json',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(bodyObj || {}),
        // optional hard timeout (2 minutes per page)
        signal: AbortSignal.timeout(120000)
      });

      const text = await resp.text();
      let json = null;
      try { json = text ? JSON.parse(text) : null; } catch (_) {}

      return { status: resp.status, ok: resp.ok, text, json };
    } catch (err) {
      const msg = String(err && err.message || err || '').toLowerCase();
      const isSocket    = msg.includes('und_err_socket') || msg.includes('other side closed');
      const isTimeout   = msg.includes('timeout');
      const isTransient = isSocket || isTimeout;

      if (attempt >= maxAttempts || !isTransient) {
        console.error('psFetchJsonPost: giving up', {
          url,
          attempt,
          transient: isTransient,
          error: String(err)
        });
        throw err;
      }

      const delay = Math.min(5000, 500 * Math.pow(2, attempt - 1)); // 0.5s, 1s, 2s, 4s...
      console.warn(`psFetchJsonPost: transient error on attempt ${attempt}/${maxAttempts}: ${err}. Retrying in ${delay}ms`);
      await sleep(delay);
    }
  }

  throw new Error('psFetchJsonPost: exhausted retries unexpectedly');
}

//PowerQuery URL + paging helpers
function buildPowerQueryUrl(queryName, qsParams, usePartner) {
  const base = `${CONFIG.psBaseUrl}${usePartner ? '/ws/partners/query/' : '/ws/schema/query/'}${encodeURIComponent(queryName)}`;
  const parts = [];
  Object.keys(qsParams || {}).forEach(k => {
    const v = qsParams[k];
    if (v !== undefined && v !== null && v !== '') {
      parts.push(`${encodeURIComponent(k)}=${encodeURIComponent(v)}`);
    }
  });
  return parts.length ? `${base}?${parts.join('&')}` : base;
}

async function postPowerQueryPage(queryName, bodyArgsObj, page, pagesize) {
  const attempts = [
    { usePartner: !!CONFIG.usePartnerQueryEndpoint },
    { usePartner: !CONFIG.usePartnerQueryEndpoint }
  ];
  let lastErr = null;

  for (const a of attempts) {
    const url = buildPowerQueryUrl(queryName, {
      page: String(page),
      pagesize: String(pagesize)
    }, a.usePartner);

    const res = await psFetchJsonPost(url, bodyArgsObj);
    if (res.status === 404 || res.status === 405) {
      lastErr = new Error(`${res.status}: ${res.text}`);
      continue;
    }
    if (res.status >= 200 && res.status < 300) {
      return res.json;
    }
    throw new Error(`POST ${a.usePartner ? 'partners' : 'schema'} failed (${res.status}) ${url}\n${res.text}`);
  }
  throw new Error(`All PowerQuery POST attempts failed for "${queryName}" (page ${page}). Last error: ${lastErr}`);
}

async function getAllPowerQueryRows(queryName, bodyArgsObj) {
  const out = [];
  let page = 1;
  const MAX_PAGES = 200; // safety guard

  while (true) {
    console.log(`PowerQuery ${queryName}: fetching page ${page}`);
    const resp = await postPowerQueryPage(queryName, bodyArgsObj || {}, page, CONFIG.pageSize);
    const recs = (resp && resp.record) || [];
    console.log(`PowerQuery ${queryName}: page ${page} returned ${recs.length} records`);

    if (!recs.length) break;
    out.push(...recs);
    if (recs.length < CONFIG.pageSize) break;

    if (page >= MAX_PAGES) {
      console.warn(`PowerQuery ${queryName}: hit MAX_PAGES=${MAX_PAGES}, stopping.`);
      break;
    }

    await sleep(CONFIG.sleepMsBetweenPages);
    page++;
  }
  return out;
}

//noramlizer & CSV writer (Node version)

function normalizeForHeaders(rows, headers, tablePrefix) {
  const prefDot = (tablePrefix || '').toLowerCase() + '.';

  return (rows || []).map(r => {
    const lut = {};
    Object.keys(r || {}).forEach(k => {
      const kl = String(k).toLowerCase();
      lut[kl] = r[k];
    });

    const out = {};
    for (const h of headers) {
      const hl = String(h).toLowerCase();
      let v =
        lut[hl] ??
        lut[prefDot + hl] ??
        r[h] ??
        (tablePrefix ? r[tablePrefix + '.' + h] : undefined) ??
        lut[hl.replace(/\./g, '')];

      out[h] = (v === undefined || v === null) ? '' : v;
    }
    return out;
  });
}

function csvEscape(val) {
  if (val === null || val === undefined) return '';
  const s = String(val);
  if (s.includes('"') || s.includes(',') || s.includes('\n') || s.includes('\r')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function objectsToCsv(headers, rows) {
  const lines = [];
  lines.push(headers.join(','));
  const BATCH = 20000;
  for (let i = 0; i < rows.length; i += BATCH) {
    const slice = rows.slice(i, i + BATCH);
    for (const r of slice) {
      const line = headers.map(h => csvEscape(r[h])).join(',');
      lines.push(line);
    }
  }
  return lines.join('\n');
}

// ==== Utility date helpers (ported from GAS) ====

function parsePSDate(v) {
  if (v instanceof Date && !isNaN(v)) return v;
  const s = String(v || '').trim();
  if (!s) return null;

  let d = new Date(s);
  if (!isNaN(d)) return d;

  d = new Date(s.replace(' ', 'T'));
  if (!isNaN(d)) return d;

  const m = s.match(/^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})/);
  if (m) {
    d = new Date(`${m[1]}T${m[2]}`);
    if (!isNaN(d)) return d;
  }
  return null;
}

function toUtcDay(d) {
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

// ==== Sheets helpers ====

async function readQuarterDates(sheets) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: 'QuarterDates!B6:C6'
  });
  const values = res.data.values || [];
  if (!values.length || values[0].length < 2) {
    throw new Error('QuarterDates!B6:C6 is empty or missing');
  }
  const [startStr, endStr] = values[0].map(v => String(v || '').trim());
  if (!startStr || !endStr) {
    throw new Error('Quarter start/end missing');
  }

  const start = parsePSDate(startStr) || new Date(startStr + 'T00:00:00');
  const end = parsePSDate(endStr) || new Date(endStr + 'T23:59:59.999');

  return { start, end, startStr, endStr };
}

async function writeSheetRebuild(sheets, sheetName, headers, objects, chunkSize = 5000) {
  // 1) Fetch spreadsheet, delete old sheet (if any), add new sheet
  const ss = await sheets.spreadsheets.get({
    spreadsheetId: SPREADSHEET_ID
  });

  const existing = (ss.data.sheets || []).find(s => s.properties.title === sheetName);
  const requests = [];

  if (existing) {
    requests.push({
      deleteSheet: { sheetId: existing.properties.sheetId }
    });
  }

  requests.push({
    addSheet: { properties: { title: sheetName } }
  });

  const batchResp = await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: { requests }
  });

  const newSheetId = batchResp.data.replies.slice(-1)[0].addSheet.properties.sheetId;

  // 2) Resize grid to fit all rows/cols we will need
  const totalRowsNeeded = 1 + objects.length; // header + body
  const totalColsNeeded = Math.max(headers.length, 1);

  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      requests: [
        {
          updateSheetProperties: {
            properties: {
              sheetId: newSheetId,
              gridProperties: {
                rowCount: Math.max(1000, totalRowsNeeded),
                columnCount: Math.max(26, totalColsNeeded)
              }
            },
            fields: 'gridProperties(rowCount,columnCount)'
          }
        }
      ]
    }
  });

  // 3) Write header row
  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!A1`,
    valueInputOption: 'RAW',
    requestBody: {
      values: [headers]
    }
  });

  // 4) Write body in chunks
  const total = objects.length;
  console.log(`writeSheetRebuild(${sheetName}): writing ${total} rows in chunks of ${chunkSize}`);

  let written = 0;
  for (let i = 0; i < total; i += chunkSize) {
    const slice = objects.slice(i, i + chunkSize);
    const values = slice.map(o => headers.map(h => (o[h] == null ? '' : o[h])));
    const startRow = 2 + i; // header is row 1
    const range = `${sheetName}!A${startRow}`;

    await sheets.spreadsheets.values.update({
      spreadsheetId: SPREADSHEET_ID,
      range,
      valueInputOption: 'RAW',
      requestBody: { values }
    });

    written += slice.length;
    console.log(`writeSheetRebuild(${sheetName}): wrote ${written}/${total}`);
  }

  return newSheetId;
}

// ==== Drive helpers ====

async function getSpreadsheetParentFolderId(drive) {
  const file = await drive.files.get({
    fileId: SPREADSHEET_ID,
    fields: 'id, parents'
  });
  const parents = file.data.parents || [];
  return parents[0] || 'root';
}

async function findExportsFolderId(drive) {
  const parentId = await getSpreadsheetParentFolderId(drive);

  const res = await drive.files.list({
    q: `'${parentId}' in parents and name = '${CONFIG.csvFolderName}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false`,
    pageSize: 1,
    fields: 'files(id, name)'
  });

  const files = res.data.files || [];
  if (files.length) {
    return files[0].id;
  }

  // create if missing
  const createRes = await drive.files.create({
    requestBody: {
      name: CONFIG.csvFolderName,
      mimeType: 'application/vnd.google-apps.folder',
      parents: [parentId]
    },
    fields: 'id, name'
  });
  console.log(`Created exports folder: ${CONFIG.csvFolderName} (id=${createRes.data.id})`);
  return createRes.data.id;
}

async function overwriteCsvFileInFolder(drive, folderId, name, csvString) {
  // delete any existing file with this name
  const list = await drive.files.list({
    q: `'${folderId}' in parents and name = '${name}' and trashed = false`,
    fields: 'files(id)'
  });
  for (const f of (list.data.files || [])) {
    await drive.files.delete({ fileId: f.id });
  }

  // create new
  const createRes = await drive.files.create({
    requestBody: {
      name,
      mimeType: 'text/csv',
      parents: [folderId]
    },
    media: {
      mimeType: 'text/csv',
      body: csvString
    },
    fields: 'id, name'
  });
  console.log(`Wrote CSV ${name} (id=${createRes.data.id})`);
}


async function getFileIdByNameInFolder(drive, folderId, name) {
  const res = await drive.files.list({
    q: `'${folderId}' in parents and name = '${name}' and trashed = false`,
    pageSize: 1,
    fields: 'files(id, name)'
  });
  const files = res.data.files || [];
  return files.length ? files[0].id : null;
}

async function downloadCsvText(drive, fileId) {
  const res = await drive.files.get(
    { fileId, alt: 'media' },
    { responseType: 'text' }
  );
  // googleapis may return string or Buffer-like
  return typeof res.data === 'string' ? res.data : res.data.toString('utf8');
}

async function downloadFileAsString(drive, fileId) {
  return downloadCsvText(drive, fileId);
}

// ==== CSV parsing & filtering ====


function rowToObjectByHeaders(row, headers) {
  const obj = {};
  for (let i = 0; i < headers.length; i++) {
    obj[headers[i]] = row[i] === undefined || row[i] === null ? '' : row[i];
  }
  return obj;
}

function readAndFilterSectionsCsvByDueDate(csvText, start, end) {
  const normalized = csvText.replace(/\r\n/g, '\n');
  const lines = normalized.split('\n');
  if (!lines.length) return { sectionsFiltered: [], sectionIdSet: new Set() };

  // Parse header row only
  const headerRows = parse(lines[0], { columns: false, skip_empty_lines: true });
  if (!headerRows.length) return { sectionsFiltered: [], sectionIdSet: new Set() };

  const headers = headerRows[0];
  const lower = headers.map(h => String(h || '').toLowerCase());
  const idxMap = {};
  lower.forEach((h, i) => { idxMap[h] = i; });

  const iDue =
    idxMap['duedate'] ??
    idxMap['due_date'] ??
    idxMap['assignmentsection.duedate'];

  if (iDue == null) {
    throw new Error('DUEDATE column not found in sections CSV');
  }

  const iIdUpper = headers.indexOf('ASSIGNMENTSECTIONID');
  const iIdA = idxMap['assignmentsectionid'];
  const iIdB = idxMap['assignmentsection.assignmentsectionid'];
  const idIndices = [iIdUpper, iIdA, iIdB].filter(i => i != null && i >= 0);

  if (!idIndices.length) {
    throw new Error('ASSIGNMENTSECTIONID column not found in sections CSV');
  }

  const startDay = toUtcDay(start);
  const endDay = toUtcDay(end);

  const out = [];
  const sectionIdSet = new Set();

  const BATCH = 5000;
  let chunk = [];

  // Process body lines in chunks
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line) continue; // skip empty lines
    chunk.push(line);

    if (chunk.length === BATCH || i === lines.length - 1) {
      const rows = parse(chunk.join('\n'), { columns: false, skip_empty_lines: true });
      for (const row of rows) {
        if (!row || !row.length) continue;

        const dueRaw = row[iDue];
        const dueDate = parsePSDate(dueRaw);
        if (!dueDate) continue;

        const dueDay = toUtcDay(dueDate);
        if (dueDay < startDay || dueDay > endDay) continue;

        // find section id
        let sid = '';
        for (const iId of idIndices) {
          sid = String(row[iId] ?? '').trim();
          if (sid) break;
        }
        if (!sid) continue;

        const obj = rowToObjectByHeaders(row, headers);
        obj['ASSIGNMENTSECTIONID'] = obj['ASSIGNMENTSECTIONID'] || sid;

        out.push(obj);
        sectionIdSet.add(sid);
      }
      chunk = [];
    }
  }

  return { sectionsFiltered: out, sectionIdSet };
}

function readScoresCsvBySectionIdSet(csvText, sectionIdSet) {
  if (!sectionIdSet || !sectionIdSet.size) return [];

  const normalized = csvText.replace(/\r\n/g, '\n');
  const lines = normalized.split('\n');
  if (!lines.length) return [];

  // Parse header row
  const headerRows = parse(lines[0], { columns: false, skip_empty_lines: true });
  if (!headerRows.length) return [];

  const headers = headerRows[0];
  const lower = headers.map(h => String(h || '').toLowerCase());
  const idxMap = {};
  lower.forEach((h, i) => { idxMap[h] = i; });

  const iSidUpper = headers.indexOf('ASSIGNMENTSECTIONID');
  const iSidA = idxMap['assignmentsectionid'];
  const iSidB = idxMap['assignmentscore.assignmentsectionid'];
  const idIndices = [iSidUpper, iSidA, iSidB].filter(i => i != null && i >= 0);

  if (!idIndices.length) {
    throw new Error('ASSIGNMENTSECTIONID column not found in scores CSV');
  }

  const out = [];
  const BATCH = 5000;
  let chunk = [];

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line) continue;
    chunk.push(line);

    if (chunk.length === BATCH || i === lines.length - 1) {
      const rows = parse(chunk.join('\n'), { columns: false, skip_empty_lines: true });
      for (const row of rows) {
        if (!row || !row.length) continue;

        let sid = '';
        for (const iId of idIndices) {
          sid = String(row[iId] ?? '').trim();
          if (sid) break;
        }
        if (!sid) continue;
        if (!sectionIdSet.has(sid)) continue;

        const obj = rowToObjectByHeaders(row, headers);
        obj['ASSIGNMENTSECTIONID'] = obj['ASSIGNMENTSECTIONID'] || sid;
        out.push(obj);
      }
      chunk = [];
    }
  }

  return out;
}

//Powerschool --> CSV sync function

async function syncCsvsFromPowerSchool(drive, scoreEntryStartDate) {
  console.log('PS sync: starting');

  // Decide what date floor to use
  let floorStr;
  if (scoreEntryStartDate) {
    // scoreEntryStartDate may be a Date or a string → normalize to Date first
    let d = scoreEntryStartDate;
    if (!(d instanceof Date) || isNaN(d)) {
      d = parsePSDate(scoreEntryStartDate) || new Date(String(scoreEntryStartDate));
    }
    if (!(d instanceof Date) || isNaN(d)) {
      throw new Error('Could not parse scoreEntryStartDate: ' + scoreEntryStartDate);
    }
    floorStr = toUtcDay(d);          // yyyy-MM-dd for PowerSchool
  } else {
    // fall back to static floor in CONFIG
    floorStr = CONFIG.scoreEntryDateFloor;   // e.g. '2025-08-01'
  }

  console.log(`PS sync: using scoreEntryDateFloor = ${floorStr}`);

  // 1) Scores
  const rawScores = await getAllPowerQueryRows(
    CONFIG.queries.score_since,
    { start_date: floorStr }
  );
  console.log(`PS sync: raw scores rows = ${rawScores.length}`);

  const scoreRows = normalizeForHeaders(rawScores, H_ASSIGNMENTSCORE, 'ASSIGNMENTSCORE');
  const nonEmpty = scoreRows.filter(r =>
    Object.values(r).some(v => v !== '' && v != null)
  );
  console.log(`PS sync: non-empty scores rows = ${nonEmpty.length}`);

  // Build set of section IDs we care about (from scores)
  const sectionIdSet = new Set();
  for (const r of nonEmpty) {
    const v = r.ASSIGNMENTSECTIONID;             // from H_ASSIGNMENTSCORE
    if (v !== '' && v != null) {
      sectionIdSet.add(String(v));
    }
  }
  console.log(`PS sync: unique section IDs from scores = ${sectionIdSet.size}`);

  // 2) Sections (same floor)
  const rawSections = await getAllPowerQueryRows(
    CONFIG.queries.section_by_score_since,
    { start_date: floorStr }
  );
  console.log(`PS sync: raw sections rows = ${rawSections.length}`);

  // Normalize section rows and filter to only those whose AssignmentSectionID is in the set
  const allSections = normalizeForHeaders(rawSections, H_ASSIGNMENTSECTION, 'ASSIGNMENTSECTION');

  const sectionRows = allSections.filter(r =>
    sectionIdSet.has(String(r.AssignmentSectionID || ''))
  );
  console.log(`PS sync: filtered sections rows = ${sectionRows.length}`);

  // 3) Write CSVs into exports folder
  const folderId = await findExportsFolderId(drive);

  const scoresCsv = objectsToCsv(H_ASSIGNMENTSCORE, nonEmpty);
  await overwriteCsvFileInFolder(drive, folderId, CONFIG.csvNames.scores, scoresCsv);

  const sectionsCsv = objectsToCsv(H_ASSIGNMENTSECTION, sectionRows);
  await overwriteCsvFileInFolder(drive, folderId, CONFIG.csvNames.sections, sectionsCsv);

  console.log('PS sync: done');
}

// ==== Express routes ====

app.get('/', (req, res) => {
  res.send('Cloud Run is alive 🟢');
});

// Main “quarter rebuild” endpoint
app.get('/run', async (req, res) => {
  const started = Date.now();
  try {
    const auth = await getAuthClient();
    const sheets = getSheets(auth);
    const drive = getDrive(auth);

    const { start, end, startStr, endStr } = await readQuarterDates(sheets);
    console.log(`Quarter window: ${startStr} .. ${endStr}`);

    // Pass the Date object as the start date floor
    await syncCsvsFromPowerSchool(drive, start);

    // From here down is your “build quarter from CSVs” logic
    const folderId = await findExportsFolderId(drive);
    const scoresCsvId   = await getFileIdByNameInFolder(drive, folderId, CONFIG.csvNames.scores);
    const sectionsCsvId = await getFileIdByNameInFolder(drive, folderId, CONFIG.csvNames.sections);
    if (!scoresCsvId || !sectionsCsvId) {
      throw new Error('Required CSV files not found in Drive folder.');
    }

    const scoresCsv   = await downloadFileAsString(drive, scoresCsvId);
    const sectionsCsv = await downloadFileAsString(drive, sectionsCsvId);

    await writeQuarterToSheets(sheets, scoresCsv, sectionsCsv, start, end);

    const ms = Date.now() - started;
    const msg = `Quarter build complete: ${startStr} .. ${endStr} in ${ms}ms`;
    console.log(msg);
    res.status(200).send(msg);
  } catch (err) {
    console.error('Error in /run:', err);
    res.status(500).send(String(err));
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Listening on port ${PORT}`);
});