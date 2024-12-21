const express = require('express');
const { Kafka } = require('kafkajs'); // Correct way to import Kafka
const fs = require('fs');
const app = express();
const PORT = 3000;

// Serve static files from the "public" folder
app.use(express.static('public'));

const countryLocations = require('./countries.json');

const blips = [];


// Kafka configuration
// const kafka = new Kafka({
//     clientId: '',
//     brokers: ['kafka:9092'] 
// });


//const consumer = kafka.consumer({ groupId: 'geo-group' });


// Mock data generation function
function generateMockData() {
    const mockMessages = [
        { USERLOCATION: 'Thailand', Tag: 'Python', TagCountByLocation: 2 },
        { USERLOCATION: 'USA', Tag: 'JavaScript', TagCountByLocation: 5 },
        { USERLOCATION: 'Germany', Tag: 'Java', TagCountByLocation: 3 },
        { USERLOCATION: 'India', Tag: 'Go', TagCountByLocation: 7 },
        { USERLOCATION: 'Brazil', Tag: 'Ruby', TagCountByLocation: 1 },
        { USERLOCATION: 'Australia', Tag: 'C#', TagCountByLocation: 4 },
        { USERLOCATION: 'Canada', Tag: 'Swift', TagCountByLocation: 6 },
        { USERLOCATION: 'Russia', Tag: 'Kotlin', TagCountByLocation: 3 },
        { USERLOCATION: 'China', Tag: 'Python', TagCountByLocation: 8 },
        { USERLOCATION: 'Japan', Tag: 'TypeScript', TagCountByLocation: 5 },
        { USERLOCATION: 'South Korea', Tag: 'C++', TagCountByLocation: 2 },
        { USERLOCATION: 'France', Tag: 'Scala', TagCountByLocation: 1 },
        { USERLOCATION: 'Italy', Tag: 'PHP', TagCountByLocation: 3 },
        { USERLOCATION: 'Mexico', Tag: 'Perl', TagCountByLocation: 2 },
        { USERLOCATION: 'Spain', Tag: 'Rust', TagCountByLocation: 4 },
        { USERLOCATION: 'UK', Tag: 'Elixir', TagCountByLocation: 2 },
        { USERLOCATION: 'South Africa', Tag: 'Dart', TagCountByLocation: 3 },
        { USERLOCATION: 'Argentina', Tag: 'Haskell', TagCountByLocation: 1 },
        { USERLOCATION: 'Egypt', Tag: 'MATLAB', TagCountByLocation: 2 },
        { USERLOCATION: 'Singapore', Tag: 'Julia', TagCountByLocation: 1 },
    ];

    mockMessages.forEach((message) => {
        const country = message.USERLOCATION.trim(); // Remove unwanted characters
        const count = parseInt(message.TagCountByLocation, 10); // Ensure it's a number
        const languages = message.Tag;

        const location = countryLocations.find((location) => location.name === country);

        if (location) {
            const existing = blips.find((blip) => blip.country === country);
            if (existing) {
                existing.count = count;
                existing.languages = languages;
            } else {
                blips.push({
                    country,
                    latitude: location.latlng[0], // Latitude
                    longitude: location.latlng[1], // Longitude
                    count,
                    languages,
                });
            }
        } else {
            console.error(`Location not found for country: ${country}`);
        }
    });

    console.log('Mock data processed:', blips);
}

// Periodically generate mock data for testing
setInterval(generateMockData, 5000); // Generate data every 5 seconds

// Serve the HTML file
app.get('/', (req, res) => {
    res.sendFile(__dirname + '/index.html');
});

// Serve the country location data
app.get('/data', (req, res) => {
    // fs.readFile('./countries.json', 'utf8', (err, data) => {
    //     if (err) {
    //         console.error('Error reading country locations:', err);
    //         return res.status(500).send('Internal Server Error');
    //     }
    //     res.json(JSON.parse(data));
    // });
    res.json(blips);
});

app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});
