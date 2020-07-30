# Stock Analyzer

### Background

This repo is code for a GCP cloud function that takes stock ticks and calculates their period-over-period performance based on the adjusted close value. Which ticks and periods are of interest are based on value in a Google sheet.

### To run

- Pip install requirements
- Make sure you create `app/secrets.py` which should contain a variable `SHEETS_CREDENTIALS` for the google service account.
- In the stock-analyzer folder, run `python main.py`. The file also accepts command line variables to investigate other stocks not in the Google sheet.

### To create floud function
- Create the cloud function in the project
- After creating the secrets.py file as described above, create a ZIP file of the contents of the `stock-analyzer` directory.
- Upload the zip to the cloud function. The runner is `runner`.

Note: 
- Google cloud functions automatically install requirements in the requirements.txt folder. Otherwise, a more complex deployment architecture is needed.

