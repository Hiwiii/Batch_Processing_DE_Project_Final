#!/usr/bin/env bash

TRY_LOOP="20"

: "${AIRFLOW_HOME:="/usr/local/airflow"}"
# Set the Airflow executor type. 
: "${AIRFLOW__CORE__EXECUTOR:=${EXECUTOR:-Sequential}Executor}"

# Determine whether to load example DAGs.
if [[ -z "$AIRFLOW__CORE__LOAD_EXAMPLES" && "${LOAD_EX:=n}" == n ]]; then
  AIRFLOW__CORE__LOAD_EXAMPLES=False
fi

# Exporting essential environment variables for Airflow.
export \
  AIRFLOW_HOME \
  AIRFLOW__CORE__EXECUTOR \
  AIRFLOW__CORE__LOAD_EXAMPLES

# Install any custom Python packages from requirements.txt file.
if [ -e "/requirements.txt" ]; then
    $(which pip) install --user -r /requirements.txt
fi

# Function to wait for a specific port on a host to be ready.
wait_for_port() {
  local name="$1" host="$2" port="$3"
  local j=0
  while ! nc -z "$host" "$port" >/dev/null 2>&1 < /dev/null; do
    j=$((j+1))
    if [ $j -ge $TRY_LOOP ]; then
      echo >&2 "$(date) - $host:$port still not reachable, giving up"
      exit 1
    fi
    echo "$(date) - waiting for $name... $j/$TRY_LOOP"
    sleep 5
  done
}

# For executors other than SequentialExecutor, we need a SQL database, and here we use PostgreSQL.
if [ "$AIRFLOW__CORE__EXECUTOR" != "SequentialExecutor" ]; then
  # Check if the user has set an explicit SQL Alchemy connection string for Airflow.
  if [ -z "$AIRFLOW__CORE__SQL_ALCHEMY_CONN" ]; then
    # Default database settings corresponding to the default Docker Compose configuration.
    : "${POSTGRES_HOST:="postgres"}"
    : "${POSTGRES_PORT:="5432"}"
    : "${POSTGRES_USER:="airflow"}"
    : "${POSTGRES_PASSWORD:="airflow"}"
    : "${POSTGRES_DB:="airflow"}"
    : "${POSTGRES_EXTRAS:-""}"

    # Construct the SQLAlchemy connection string for PostgreSQL.
    AIRFLOW__CORE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}${POSTGRES_EXTRAS}"
    export AIRFLOW__CORE__SQL_ALCHEMY_CONN
  else
    # Extract PostgreSQL connection details from the SQL Alchemy connection string.
    POSTGRES_ENDPOINT=$(echo -n "$AIRFLOW__CORE__SQL_ALCHEMY_CONN" | cut -d '/' -f3 | sed -e 's,.*@,,')
    POSTGRES_HOST=$(echo -n "$POSTGRES_ENDPOINT" | cut -d ':' -f1)
    POSTGRES_PORT=$(echo -n "$POSTGRES_ENDPOINT" | cut -d ':' -f2)
  fi

  # Wait until PostgreSQL is available.
  wait_for_port "Postgres" "$POSTGRES_HOST" "$POSTGRES_PORT"
fi

# Handle different Airflow commands based on the argument passed.
case "$1" in
  webserver)
    # Initialize the Airflow database before starting the web server.
    airflow db init
    if [ "$AIRFLOW__CORE__EXECUTOR" = "LocalExecutor" ] || [ "$AIRFLOW__CORE__EXECUTOR" = "SequentialExecutor" ]; then
      # For Local and Sequential Executors, run the scheduler in the same container.
      airflow scheduler &
    fi
    exec airflow webserver
    ;;
  worker|scheduler)
    # Allow time for the web server to complete initialization.
    sleep 10
    exec airflow "$@"
    ;;
  version)
    # Output the Airflow version.
    exec airflow "$@"
    ;;
  *)
    exec "$@"
    ;;
esac
