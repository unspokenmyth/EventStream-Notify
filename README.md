# eventstream-notify 🚀

A real-time, event-driven notification system powered by Apache Kafka and Redis. This project simulates a production-style micro-event pipeline where user actions are ingested, processed, and trigger notifications, making it an excellent showcase of data engineering and backend architecture skills.

---

## 🧭 Core Idea

This system mimics how modern applications handle notifications. A **producer** sends events (like `user_signup` or `item_purchased`) to a Kafka topic. A **consumer** listens to these events, looks up user preferences from a fast Redis cache, and then sends the appropriate notification (e.g., email, SMS, or just a log message).

---

## 🧩 Architecture

The data flow is designed to be simple, scalable, and decoupled.

            ┌────────────────┐
            │ Event Producer │
            │ (Python script)│
            └──────┬─────────┘
                   │ JSON events
                   ▼
            ┌────────────────┐
            │   Kafka Topic  │
            │  "user_events" │
            └──────┬─────────┘
                   │
                   ▼
    ┌──────────────────────────┐
    │ Notification Consumer    │
    │ (Python service)         │
    │ 1. Reads from Kafka      │
    │ 2. Looks up user prefs in│
    │    Redis (e.g. email/SMS)│
    │ 3. Sends notification    │
    └──────┬───────────────────┘
           │
           ▼
    ┌──────────────────────┐
    │ Output:              │
    │  → Mock Email / SMS  │
    │  → Console Log       │
    └──────────────────────┘



    ---

## ✨ Key Features

* **Event-Driven:** Built on a producer/consumer model for a scalable and resilient system.
* **Real-Time Processing:** Instantly processes events as they arrive.
* **Data Enrichment:** Uses Redis for high-speed data lookups to enrich events with user data.
* **Modular & Extensible:** Easily add new event types (e.g., `password_reset`) or notification channels (e.g., push notifications).
* **Local-First Setup:** Runs on a single machine without Docker, making it simple to set up and demonstrate.

---

## 🛠️ Tech Stack

* **Message Broker:** Apache Kafka
* **Cache / In-Memory Database:** Redis
* **Core Logic:** Python
* **Python Libraries:** `confluent-kafka-python`, `redis-py`, `schedule`

---

## ⚙️ Getting Started

Follow these steps to get the system up and running on your local machine.

### Prerequisites

1.  **Python 3.8+** installed.
2.  **Apache Kafka** installed and binaries available in your `PATH`. You can download it [here](https://kafka.apache.org/downloads).
3.  **Redis** installed and running. You can download it [here](https://redis.io/docs/getting-started/installation/).

### Installation & Setup

1.  **Clone the repository:**
    ```sh
    git clone [https://github.com/your-username/eventstream-notify.git](https://github.com/your-username/eventstream-notify.git)
    cd eventstream-notify
    ```

2.  **Create a virtual environment and install dependencies:**
    ```sh
    # For macOS/Linux
    python3 -m venv venv
    source venv/bin/activate

    # For Windows
    python -m venv venv
    .\venv\Scripts\activate

    # Install packages
    pip install -r requirements.txt
    ```

### Running the System

1.  **Start Kafka & Redis (Manual Steps):**
    * Open a terminal and start the ZooKeeper server.
    * Open another terminal and start the Kafka server.
    * Ensure your Redis server is running (`redis-server`).

2.  **Launch the Automation Script:**
    * Once the services above are running, open a new terminal, activate the virtual environment, and run the main controller script:
    ```sh
    python main_controller.py
    ```

3.  **What the script does:**
    * Checks for and creates the `user_events` Kafka topic if it doesn't exist.
    * Seeds Redis with sample user data and notification preferences.
    * Starts the notification **consumer** in a background thread.
    * Starts the event **producer**, which will periodically send mock events to Kafka.

4.  **Observe the Output:**
    * Watch the terminal! You will see logs from the consumer indicating that it has received an event, fetched user data from Redis, and "sent" a notification to the console.

---

## 🔧 Extensibility

Want to add a new feature? It's easy!

* **Add a new event type:** Simply add a new event generator function in `producer.py`.
* **Add a new notification channel:** Add a `send_push_notification()` function in `consumer.py` and update the logic to check user preferences for this new channel.
