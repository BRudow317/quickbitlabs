#!/usr/bin/env bash

# Filename is the input arg
FileName=$1

# env to parse the .env for
env=dev
source ~/.env "$env"

# Bash indirect expansion: ${!var} replaces the eval echo logic
oracle_home=${!oracle_home}
sid=${!oracle_sid}
username=${!oracle_user}
password=${!oracle_pass}

export ORACLE_HOME="$oracle_home"
export ORACLE_SID="$oracle_sid"
export PATH="$PATH:$ORACLE_HOME/bin"

package_output=$(sqlplus -s "$username"/"$password" << SCRIPT
SET SERVEROUTPUT ON
SET FEED OFF
DECLARE 
job_text varchar2(100);
return_text varchar2(100) := 'Return Code:';
BEGIN
job_text:=$username.$package.$function('$FileName');
dbms_output.put_line(return_text);
dbms_output.put_line(job_text);
END;
/
SCRIPT
)

# Parsing the output. Quoting "$package_output" preserves newlines for awk.
exit_code=$(echo "$package_output" | awk -F'Return Code: ' '{print $2}')


if [[ "$exit_code" -eq 0 ]]; then
    echo "Job ran successfully."
    exit 0
else
    echo "Job failed. Check the log."
    exit 1
fi