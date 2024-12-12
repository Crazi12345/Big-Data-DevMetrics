const countryCoordinates = {
    "United States": { lat: 37.0902, lng: -95.7129 },
    "India": { lat: 20.5937, lng: 78.9629 },
    "Brazil": { lat: -14.2350, lng: -51.9253 }
    // Add more countries as needed
};


// Initialize map
const map = L.map('map').setView([20, 0], 2); // Centered on the world
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png').addTo(map);

// Fetch mock data and plot it
fetch('/data')
    .then(response => response.json())
    .then(data => {
        // Convert data into heatmap format
        const heatmapData = data.map(entry => ({
            lat: countryCoordinates[entry.country].lat,
            lng: countryCoordinates[entry.country].lng,
            value: entry.developers
        }));

        // Initialize heatmap layer
        const heatmapLayer = new HeatmapOverlay({
            radius: 25,
            maxOpacity: 0.8,
            scaleRadius: true,
            useLocalExtrema: true,
            latField: 'lat',
            lngField: 'lng',
            valueField: 'value'
        });
        map.addLayer(heatmapLayer);
        heatmapLayer.setData({ data: heatmapData });
    });
