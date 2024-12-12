const express = require('express');
const app = express();
const PORT = 3000;

app.use(express.static('public'));

// Serve the HTML file
app.get('/', (req, res) => {
    res.sendFile(__dirname + '/index.html');
});

// Serve mock data
app.get('/data', (req, res) => {
    res.json(require('./mock-data.json'));
});

app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});
