const express = require('express');
const path = require('path');

const jokeApiRouter = require('./routes/jokeApi');

const app = express();
const PORT = process.env.PORT || 3000;

app.use('/', jokeApiRouter);

app.use(express.static(path.join(__dirname, 'public'))); //serve static files from public directory

app.listen(PORT, "0.0.0.0", () => {
    console.log(`Server is running on port ${PORT}`);
});

