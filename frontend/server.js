'use strict';

const express = require('express');
const axios = require("axios").create();

// Constants
const PORT = 8080;
const HOST = '0.0.0.0';

// App
const app = express();
app.use(express.static( __dirname + '/public'));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.disable('x-powered-by');
app.use((err, req, res, next) => { // eslint-disable-line no-unused-vars
      if (!err.status && !err.errors) {
        res.status(500).json({ error: [{ message: err.message }] });
      } else {
        res.status(err.status).json({ errors: err.message });
      }
    });

app.get('/search_as_you_type', async (req, res) => {
    const { search_phrase, index, size } = req.query;
    try {
		const response = await axios.get("http://10.133.86.152:8000/search_as_you_type",{
			params: { search_phrase, index, size }
            });
		res.status(200).json({ results: response.data });
	} catch (err) {
	    console.log(err);
		res.status(500).json({ message: err });
	}
});

app.get('/search', async (req, res) => {
    const { search_phrase, index, size } = req.query;
    try {
		const response = await axios.get("http://10.133.86.152:8000/search",{
			params: { search_phrase, index, size }
            });
		res.status(200).json({ results: response.data });
	} catch (err) {
	    console.log(err);
		res.status(500).json({ message: err });
	}
});

app.listen(PORT, HOST, () => {
  console.log(`Running on http://${HOST}:${PORT}`);
});