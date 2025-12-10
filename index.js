const express = require('express');
const { google } = require('googleapis');
const { parse } = require('csv-parse/sync');

const app = express();

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
      'https://www.googleapis.com/auth/drive.readonly'
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
    q: `'${parentId}' in parents and name = '${EXPORT_FOLDER_NAME}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false`,
    pageSize: 1,
    fields: 'files(id, name)'
  });

  const files = res.data.files || [];
  if (!files.length) {
    throw new Error(`Exports folder "${EXPORT_FOLDER_NAME}" not found under spreadsheet parent.`);
  }
  return files[0].id;
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

    const folderId = await findExportsFolderId(drive);

    const scoresId = await getFileIdByNameInFolder(drive, folderId, SCORES_CSV_NAME);
    const sectionsId = await getFileIdByNameInFolder(drive, folderId, SECTIONS_CSV_NAME);

    if (!scoresId || !sectionsId) {
      throw new Error(`Missing CSVs. scoresId=${scoresId}, sectionsId=${sectionsId}`);
    }

    const sectionsCsv = await downloadCsvText(drive, sectionsId);
    const scoresCsv = await downloadCsvText(drive, scoresId);

    const { sectionsFiltered, sectionIdSet } =
      readAndFilterSectionsCsvByDueDate(sectionsCsv, start, end);
    const scoresFiltered =
      readScoresCsvBySectionIdSet(scoresCsv, sectionIdSet);

    console.log(`Filtered sections: ${sectionsFiltered.length}, scores: ${scoresFiltered.length}`);

    await writeSheetRebuild(sheets, 'AssignmentSectionQuarterSpecific', H_ASSIGNMENTSECTION, sectionsFiltered);
    await writeSheetRebuild(sheets, 'AssignmentScore', H_ASSIGNMENTSCORE, scoresFiltered);

    const ms = Date.now() - started;
    const msg = `Quarter build complete: sections=${sectionsFiltered.length}, scores=${scoresFiltered.length}, ms=${ms}`;
    console.log(msg);
    res.status(200).send(msg);
  } catch (err) {
    console.error(err);
    res.status(500).send(String(err));
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Listening on port ${PORT}`);
});