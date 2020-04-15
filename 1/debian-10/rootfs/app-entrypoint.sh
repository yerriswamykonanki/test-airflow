#!/usr/bin/env bash
. /opt/bitnami/base/functions
. /opt/bitnami/base/helpers

: "${AIRFLOW__CORE__FERNET_KEY:=${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")}}"

export \
  AIRFLOW__CORE__FERNET_KEY \
  AIRFLOW__CORE__LOAD_EXAMPLES \


#if [ -e "/requirements.txt" ]; then
#    $(command -v pip) install --user -r /requirements.txt
#fi


# Load DAGs exemples (default: Yes)
#if [[ -z "$AIRFLOW__CORE__LOAD_EXAMPLES" && "${LOAD_EX:=n}" == n ]]
#then
  AIRFLOW__CORE__LOAD_EXAMPLES=False
#fi

print_welcome_page

if [[ "$1" == "nami" && "$2" == "start" ]] || [[ "$1" == "/run.sh" ]]; then
    if [ ! $EUID -eq 0 ] && ! getent passwd "$(id -u)" &> /dev/null && [ -e /usr/lib/libnss_wrapper.so ]; then
    echo "airflow:x:$(id -u):$(id -g):Airflow:$AIRFLOW_HOME:/bin/false" > "$NSS_WRAPPER_PASSWD"
    echo "airflow:x:$(id -g):" > "$NSS_WRAPPER_GROUP"
else
    unset LD_PRELOAD
fi

    nami_initialize airflow
    info "Starting airflow... "
fi

exec tini -- "$@"
