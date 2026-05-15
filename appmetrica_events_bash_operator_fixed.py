# Paste this BashOperator body into the DAG where `dag` and upstream
# credential tasks are already defined.
events = BashOperator(
    task_id="events",
    bash_command=r"""
set -euo pipefail

# Parameters
START_DATE="{{ (macros.datetime.strptime(dag_run.conf['start_date'] if dag_run and dag_run.conf and dag_run.conf.get('start_date') else macros.datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d') - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"
END_DATE="{{ (macros.datetime.strptime(dag_run.conf['end_date'] if dag_run and dag_run.conf and dag_run.conf.get('end_date') else macros.datetime.now().strftime('%Y-%m-%d'), '%Y-%m-%d') - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"

AM_TOKEN="{{task_instance.xcom_pull(key='yametrica_token-token', task_ids=['get_yametrica_token'])[0]}}"
APP_ID="2931871"

CH_USER="{{ task_instance.xcom_pull(key='tvbigdata_clickhouse-username', task_ids=['get_ch_creds'])[0] }}"
CH_PASSWORD="{{ task_instance.xcom_pull(key='tvbigdata_clickhouse-password', task_ids=['get_ch_creds'])[0] }}"

CH_HOST="tvbigdata-50inz-s01-r01.dbms-prod-click.cloud.vimpelcom.ru"
CH_TABLE="sandbox.appm_events"

PROXY_HOST="ms-mwgsrv.bee.vimpelcom.ru"
PROXY_PORT="9090"
PROXY_USER="{{ task_instance.xcom_pull(key='tech_yametrics_ms-username', task_ids=['get_proxy_creds'])[0] }}"
PROXY_PASS="{{ task_instance.xcom_pull(key='tech_yametrics_ms-password', task_ids=['get_proxy_creds'])[0] }}"

FIELDS="event_datetime,event_name,event_json,event_timestamp,event_receive_datetime,session_id,installation_id,appmetrica_device_id,profile_id,device_type,os_name,os_version,country_iso_code,city,app_build_number,app_package_name,app_version_name,application_id"
EXPORT_ENDPOINT="https://api.appmetrica.yandex.com/logs/v1/export/events.csv"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/appmetrica_events.XXXXXX")"
trap 'rm -rf "$WORK_DIR"' EXIT

curl_appmetrica() {
    local output_file="$1"
    local date_since="$2"
    local date_until="$3"
    local http_code
    local curl_rc

    set +e
    http_code="$(curl -sS -o "$output_file" -w "%{http_code}" \
        -G "$EXPORT_ENDPOINT" \
        --data-urlencode "application_id=${APP_ID}" \
        --data-urlencode "date_since=${date_since}" \
        --data-urlencode "date_until=${date_until}" \
        --data-urlencode "skip_unavailable_shards=true" \
        --data-urlencode "fields=${FIELDS}" \
        -k \
        --proxy "${PROXY_HOST}:${PROXY_PORT}" \
        --proxy-anyauth \
        --proxy-insecure \
        --proxy-user "${PROXY_USER}:${PROXY_PASS}" \
        -H "Authorization: OAuth ${AM_TOKEN}" \
        -H "Accept: text/csv")"
    curl_rc="$?"
    set -e

    if [ "$curl_rc" -ne 0 ] && [ -z "$http_code" ]; then
        http_code="000"
    fi

    printf '%s' "$http_code"
}

DAY="$START_DATE"
while [[ "$DAY" < "$END_DATE" ]] || [[ "$DAY" == "$END_DATE" ]]; do
    echo "=== DAY: $DAY ==="
    DAY_CLEAR="${DAY//-/}"

    echo "DELETE FROM $CH_TABLE WHERE toDate(toDateTime(event_datetime, 'UTC'), 'Europe/Moscow') = '$DAY'" | \
        curl -k --fail --show-error --silent \
            --user "$CH_USER:$CH_PASSWORD" \
            --data-binary @- \
            "http://$CH_HOST:8123/"

    for HOUR in {00..23}; do
        for MINUTE in 00 30; do
            ABS_HOUR_START="$DAY $HOUR:$MINUTE:00"
            if [ "$MINUTE" = "00" ]; then
                ABS_HOUR_END="$DAY $HOUR:29:59"
            else
                ABS_HOUR_END="$DAY $HOUR:59:59"
            fi

            OUTPUT_FILE="$WORK_DIR/appmetrica_events_${DAY_CLEAR}_${HOUR}_${MINUTE}.csv"

            ATTEMPT=1
            MAX_ATTEMPTS=120
            while true; do
                HTTP_CODE="$(curl_appmetrica "$OUTPUT_FILE" "$ABS_HOUR_START" "$ABS_HOUR_END")"

                if [ "$HTTP_CODE" = "200" ]; then
                    echo "CSV ready for range $ABS_HOUR_START - $ABS_HOUR_END"
                    break
                elif [ "$HTTP_CODE" = "202" ]; then
                    if [ "$ATTEMPT" -ge "$MAX_ATTEMPTS" ]; then
                        echo "!!! AppMetrica data was not ready after $MAX_ATTEMPTS attempts"
                        echo "Range: $ABS_HOUR_START - $ABS_HOUR_END"
                        exit 1
                    fi
                    echo "Data is still preparing... attempt $ATTEMPT/$MAX_ATTEMPTS"
                    ATTEMPT=$((ATTEMPT + 1))
                    sleep 5
                else
                    echo "!!! HTTP error from AppMetrica: $HTTP_CODE"
                    echo "Range: $ABS_HOUR_START - $ABS_HOUR_END"
                    if [ -s "$OUTPUT_FILE" ]; then
                        sed -n '1,40p' "$OUTPUT_FILE"
                    fi
                    exit 1
                fi
            done

            if [ ! -s "$OUTPUT_FILE" ]; then
                echo "Empty CSV for range $ABS_HOUR_START - $ABS_HOUR_END, skipping"
                continue
            fi

            ROWS="$(python3 - "$OUTPUT_FILE" <<'PY'
import csv
import sys

with open(sys.argv[1], newline='', encoding='utf-8-sig') as csv_file:
    count = sum(1 for _ in csv.reader(csv_file))

print(max(count - 1, 0))
PY
)"

            if [ "$ROWS" = "0" ]; then
                echo "No data rows in CSV for range $ABS_HOUR_START - $ABS_HOUR_END, skipping"
                rm -f "$OUTPUT_FILE"
                continue
            fi

            echo "Created $ROWS CSV rows"
            echo "Loading into ClickHouse..."
            curl -k --fail --show-error --silent \
                --user "$CH_USER:$CH_PASSWORD" \
                --data-binary @"$OUTPUT_FILE" \
                "http://$CH_HOST:8123/?query=INSERT%20INTO%20$CH_TABLE%20FORMAT%20CSVWithNames"

            rm -f "$OUTPUT_FILE"
        done
    done

    DAY="$(date -I -d "$DAY + 1 day" 2>/dev/null || date -d "$DAY +1day" '+%Y-%m-%d')"
done

echo "=== Completed! ==="
""",
    dag=dag,
)
