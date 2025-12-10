const express = require('express');
const { google } = require('googleapis');

const app = express();

app.get('/', (req, res) => {
  res.send('Cloud Run is alive 🟢');
});

app.get('/run', async (req, res) => {
  try {
    res.status(200).send('Gradebook runner stub: /run hit OK');
  } catch (err) {
    console.error(err);
    res.status(500).send(String(err));
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Listening on port ${PORT}`);
});