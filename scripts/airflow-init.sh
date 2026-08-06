#!/bin/bash

set -e


echo "Migrating database..."

airflow db migrate


echo "Checking admin user..."

if airflow users list | grep -q "${AIRFLOW_ADMIN_USERNAME}"; then
    echo "Admin user already exists"
else

    airflow users create \
        --username "${AIRFLOW_ADMIN_USERNAME}" \
        --password "${AIRFLOW_ADMIN_PASSWORD}" \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email "${AIRFLOW_ADMIN_EMAIL}"

fi