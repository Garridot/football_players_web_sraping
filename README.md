# Football Players Web Scraper & Stats Processor ⚽📊

![Python](https://img.shields.io/badge/python-3.8%2B-blue)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-3.9%2B-orange)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-enabled-green)

This automated ETL pipeline extracts player statistics from Transfermarkt, transforms the data, and loads it into a REST API using message queuing for scalable processing.

## Project Structure 🗂️

```plaintext
/project
├── main.py            # Producer: sends tasks to RabbitMQ
├── consumer.py        # Consumer: processes scraping tasks
├── scrapper.py        # Web scraping logic with BeautifulSoup
├── config.py          # Configuration (environment variables)
├── auth.py            # API authentication
├── data_manager.py    # Data processing (Pandas)
├── logger_config.py   # Logging configuration
├── save_data.py       # API saving functions
├── requirements.txt   # Dependencies
└── .github/workflows  # CI/CD with GitHub Actions
```
### Prerequisites 📋

* Python 3.8+
* CloudAMQP account (free plan available)

### Configuration ⚙️

1.  VEnvironment variables: Create a ```.env``` file based on this example:
```plaintext
# .env.example
# .env.example
API_AUTH_URL=https://your-api.com/auth/
API_PLAYERS_URL=https://your-api.com/players/
API_PLAYER_STATS_URL=https://your-api.com/stats/
API_STATS_BY_POSITION_URL=https://your-api.com/position-stats/
EMAIL=your_email@api.com
PASSWORD=your_password
CLOUDAMQP_URL=amqps://user:pass@host/vhost
```
2. Install dependencies:
```pip 
pip install -r requirements.txt
```

### Usage 🚀
#### Local Execution
1. Producer (sends tasks):
```bash
python main.py
```
2. Consumer (processes tasks):
```bash
python consumer.py
```
#### Production Execution
* System runs automatically daily at midnight UTC via GitHub Actions.
* Consumers can be scaled in parallel for better performance.

### 🧩 Core Components
#### Producer (main.py)
* Creates scraping tasks
* Manages RabbitMQ connections
* Handles task prioritization

#### Consumer (consumer.py)
* Processes tasks from queue
* Implements exponential backoff for retries
* Manages API authentication

### Architecture 🏗️
```mermaid
sequenceDiagram
    Producer->>+RabbitMQ: Enqueue scraping task
    RabbitMQ->>+Consumer: Deliver task
    Consumer->>+Transfermarkt: Scrape data
    Transfermarkt-->>-Consumer: Return HTML
    Consumer->>+DataProcessor: Clean/transform
    DataProcessor-->>-Consumer: Structured data
    Consumer->>+API: POST processed data
```
### GitHub Actions Workflow 🤖
```yaml
name: Cron Job
on:
  schedule:
    - cron: '0 0 * * *'  # Daily execution at midnight UTC
jobs:
  run_producer:
    # Sends tasks to RabbitMQ
  run_consumer:
    needs: run_producer  # Sequential dependency
    # Processes tasks with 3 parallel workers
```
### Error Handling ⚠️
* Automatic retries on connection failures
* Persistent queues in RabbitMQ

### Future Improvements 🔮
* Add monitoring with Prometheus/Grafana
* Implement dead-letter queue for failed messages
* Dockerize the application

### Contributing 🤝
1. Fork the project
2. Create your branch (```git checkout -b feature/fooBar```)
3. Commit your changes (```git commit -am 'Add some fooBar'```)
4. Push to the branch (```git push origin feature/fooBar```)
5. Open a Pull Request
