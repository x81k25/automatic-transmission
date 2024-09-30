# automatic-transmission
The purpose of this project is to create a back-end service to be deployed from a docker container that will ingest RSS feeds, process those feeds, and then feed them into the application Transmission.

## set-up

### install transmission
The linux application Transmission will be required to be installed on the host machine. To install transmission, run the following commands:
```commandline
sudo apt update
sudo apt install transmission-daemon
```

### configure transmission
```commandline
sudo systemctl stop transmission-daemon
sudo nano /etc/transmission-daemon/settings.json
```

Likely parameters to modify:

```"rpc-enabled": true,```: Ensure remote access is enabled.

```"rpc-bind-address": "0.0.0.0",```: Set this to 0.0.0.0 to allow access from any IP (or restrict it to a specific IP).

```"rpc-whitelist": "127.0.0.1",```: Replace 127.0.0.1 with your local network IP range (or remove it if you don't want a whitelist).

```"rpc-authentication-required"```: true,: Set this to true to require a username and password.

```"rpc-username": "your_username",```: Set your desired username.

```"rpc-password": "your_password",```: Set your desired password (it will be hashed after the daemon is restarted).

### install python packages
Pip install all packages located within requirements.txt

### environemnt variables
Create a .env file in the root directory of the project with the following variables:
```commandline
server-ip
server-username
server-password
transmission-username
transmission-password
```

## file structure

```
my-python-project/
├── app/                    # Main application source code
│   ├── __init__.py         # Makes app a package
│   ├── main.py             # Entry point for the application
│   ├── utils.py            # Utility functions (example)
│   └── ...                 # Other modules
├── tests/                  # Unit and integration tests
│   ├── __init__.py
│   └── test_main.py        # Test cases for main.py
├── Dockerfile              # Docker configuration file
├── docker-compose.yml      # Docker Compose file (optional)
├── .dockerignore           # Ignore unnecessary files in the Docker image
├── requirements.txt        # List of Python dependencies
├── .env                    # Environment variables (ignored by version control)
├── README.md               # Project documentation
├── setup.py                # Installation and package details (optional)
├── config/                 # Configuration files (optional)
│   └── config.yaml         # Example configuration file
├── .gitignore              # Ignore specific files in Git
└── LICENSE                 # License for the project (e.g., MIT)
```

## Key Files and Directories:
1. app/ (Main Application Code)

    __init__.py: Marks this directory as a package.
    main.py: The main script that runs the application (entry point).
    utils.py: A sample module for utility functions (you can have other modules based on your project’s needs).

2. tests/ (Test Suite)

    test_main.py: Example of unit or integration tests for your application.
    Tests are important for ensuring that your code behaves as expected.

3. Dockerfile

    Defines how the Docker image is built, including the base image, dependencies, and how to run the application.

4. docker-compose.yml

    Defines multi-container Docker applications, useful if your app relies on other services like databases, message queues, etc.

5. .dockerignore

    Similar to .gitignore, this file prevents unnecessary files from being included in your Docker image (e.g., tests/, .git/, etc.).

6. requirements.txt

    Lists Python dependencies. Use pip freeze > requirements.txt to generate this file.

7. .env

    Stores environment variables (e.g., API keys, database URIs), ignored in version control for security.

8. README.md

    A clear explanation of your project, how to set it up, and how to use it.

9. setup.py (Optional)

    If you plan to distribute your application as a package, setup.py provides metadata about your project and helps with packaging and distribution.

