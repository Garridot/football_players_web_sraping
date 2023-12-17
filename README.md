# Football Players Stats Api

This Python web scraping project focuses on extracting football player statistics from the [Transfermarkt](https://www.transfermarkt.us/) website. The main script, **main.py**, uses the BeautifulSoup library to parse the HTML content of web pages and extract the necessary information. The schedule to run this script is configured using GitHub Actions.

## Project Structure
- .github/workflows
    - cron.yml
- .gitignore
- main.py
- requirements.txt
- Readme.md

## Structure Description
- **.github/workflows/cron.yml**: GitHub Actions configuration file that sets the schedule to run the web scraping script at midnight every day.

- **.gitignore**: File specifying files and directories to be ignored when committing to Git.

- **main.py**: The main script that performs web scraping of football player statistics. It utilizes BeautifulSoup, pandas, and requests.

- **requirements.txt**: File listing project dependencies and their versions.

## Running the Script
The main.py script extracts data from various football players and updates the statistics on a local API. Make sure to have the dependencies installed by running:

```
pip install -r requirements.txt
pip install lxml
```
Then, execute the script:
```
python main.py
```

The script will extract player statistics and update them in [Football Players Stats Api](https://github.com/Garridot/football-players-stats-api).