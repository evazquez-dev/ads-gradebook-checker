const express = require('express');
const { google } = require('googleapis');

const app = express();

// COPY these from your GAS file:
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

// Spreadsheet & folder IDs from your CONFIG
const SPREADSHEET_ID = '1zPu05Vi6m_kt5PvF0n0caWFJf_P3FGs4qed18493UlA';
// You can hardcode the export folder ID or later look it up by name
const EXPORT_FOLDER_ID = 'PUT_YOUR_FOLDER_ID_HERE';

async function getAuthClient() {
  const auth = new google.auth.GoogleAuth({
    scopes: [
      'https://www.googleapis.com/auth/spreadsheets',
      'https://www.googleapis.com/auth/drive.readonly'
    ]
  });
  return await auth.getClient();
}

async function readQuarterDates(authClient) {
  const sheets = google.sheets({ version: 'v4', auth: authClient });
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SPREADSHEET_ID,
    range: 'QuarterDates!B6:C6'
  });
  const values = res.data.values || [];
  if (!values.length || values[0].length < 2) {
    throw new Error('QuarterDates!B6:C6 is empty or missing');
  }
  const [start, end] = values[0].map(v => String(v || '').trim());
  if (!start || !end) {
    throw new Error('Quarter start/end missing');
  }
  return { start, end };
}

async function writeSheet(authClient, sheetName, headers, rows) {
  const sheets = google.sheets({ version: 'v4', auth: authClient });

  // Get current sheet info
  const ss = await sheets.spreadsheets.get({
    spreadsheetId: SPREADSHEET_ID
  });

  const sheet = (ss.data.sheets || []).find(s => s.properties.title === sheetName);

  // Delete sheet if exists
  if (sheet) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: SPREADSHEET_ID,
      requestBody: {
        requests: [{ deleteSheet: { sheetId: sheet.properties.sheetId } }]
      }
    });
  }

  // Add new sheet
  await sheets.spreadsheets.batchUpdate({
    spreadsheetId: SPREADSHEET_ID,
    requestBody: {
      requests: [{
        addSheet: { properties: { title: sheetName } }
      }]
    }
  });

  const values = [headers, ...rows.map(o => headers.map(h => o[h] == null ? '' : o[h]))];

  await sheets.spreadsheets.values.update({
    spreadsheetId: SPREADSHEET_ID,
    range: `${sheetName}!A1`,
    valueInputOption: 'RAW',
    requestBody: { values }
  });
}

// TODO: helper to read a CSV file from Drive, similar to readCsvAsObjects_ in GAS
async function readCsvFromDrive(authClient, fileId) {
  const drive = google.drive({ version: 'v3', auth: authClient });
  const res = await drive.files.get(
    { fileId, alt: 'media' },
    { responseType: 'text' }
  );
  const text = res.data;
  const lines = text.replace(/\r\n/g, '\n').split('\n');
  if (!lines.length) return [];

  const headers = parseCsvLine(lines[0]);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    if (!lines[i]) continue;
    const cols = parseCsvLine(lines[i]);
    const obj = {};
    headers.forEach((h, idx) => {
      obj[h] = cols[idx] ?? '';
    });
    rows.push(obj);
  }
  return rows;
}

// VERY simple CSV splitter (no quoted commas handling). 
// For robust parsing, use a CSV library later.
function parseCsvLine(line) {
  return line.split(',');
}

app.get('/run', async (req, res) => {
  try {
    const authClient = await getAuthClient();

    // 1) Get quarter window
    const { start, end } = await readQuarterDates(authClient);

    // 2) TODO: locate CSVs by name in EXPORT_FOLDER_ID via Drive API,
    //    similar to getFileIdByNameInFolder_ in GAS.
    // 3) TODO: read & filter sections CSV by DUEDATE window,
    //    like readAndFilterSectionsCsvByDueDate_.
    // 4) TODO: read & filter scores CSV by section IDs,
    //    like readScoresCsvBySectionIdSet_.
    const sectionsFiltered = []; // placeholder
    const scoresFiltered = [];   // placeholder

    // 5) Write sheets (single-write rebuilds)
    await writeSheet(authClient, 'AssignmentSectionQuarterSpecific', H_ASSIGNMENTSECTION, sectionsFiltered);
    await writeSheet(authClient, 'AssignmentScore', H_ASSIGNMENTSCORE, scoresFiltered);

    res.status(200).send(`Quarter tabs rebuilt for window ${start} .. ${end}`);
  } catch (err) {
    console.error(err);
    res.status(500).send(String(err));
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Listening on port ${PORT}`);
});