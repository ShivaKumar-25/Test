import requests
import json
import time
import logging
from typing import List, Optional
import sqlparse

# --------------------------------------
# Logger
# --------------------------------------
logger = logging.getLogger("proc_converter_simple")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logger.addHandler(ch)

IMPROVED_PROMPT = r"""
You are a SQL conversion engine. Convert the input SQL Server stored procedure into a valid Databricks SQL stored procedure.  
Return ONLY the converted Databricks SQL code:

<converted SQL here>

No explanations, no comments, no prose, no markdown outside the code block.

--------------------------------------------------------------------
CONVERSION RULES
--------------------------------------------------------------------

1. PROCEDURE DEFINITION  
Use the exact Databricks SQL stored procedure syntax:  

CREATE [OR REPLACE | IF NOT EXISTS] PROCEDURE procedure_name(parameter_list)  
    LANGUAGE SQL  
    SQL SECURITY INVOKER  
AS  
BEGIN  
    ...  
END;  

Procedure parameters must use IN, OUT, or INOUT.  
Do NOT include any @ prefix.  
If the original SQL Server procedure has default parameter values, you may preserve them using the DEFAULT clause (optional).  

2. DATA TYPE CONVERSIONS  
Map SQL Server types to Databricks SQL types:  
- VARCHAR, NVARCHAR, CHAR, NCHAR, TEXT → STRING  
- INT → INT  
- BIGINT → BIGINT  
- SMALLINT → SMALLINT  
- TINYINT → SMALLINT  
- DECIMAL(p,s), NUMERIC(p,s), MONEY, SMALLMONEY → DECIMAL(p,s)  
- FLOAT, REAL → DOUBLE  
- BIT → BOOLEAN  
- DATETIME, DATETIME2, SMALLDATETIME → TIMESTAMP  
- DATE → DATE  
- TIME → STRING  
- UNIQUEIDENTIFIER → STRING  
- VARBINARY, BINARY → BINARY  

3. PARAMETERS  
Remove any @ prefixes on parameter names.  
If SQL Server parameters had default values, you may map them to Databricks using:  
    parameter_name data_type DEFAULT default_expression  

4. VARIABLES  
Declare variables inside the procedure body as follows:  
    DECLARE var_name DATA_TYPE DEFAULT initial_value;  

Assign values using SET or SELECT into, for example:  
    SET (var1, var2, ...) = (SELECT ...);  

Notes for SELECT assignment:  
- Databricks SQL does NOT support T-SQL style SELECT … INTO for assigning variables.  
- If the original SQL Server procedure uses SELECT … INTO variables, convert it to:  
      SET (var1, var2, ...) = (SELECT column1, column2, ... FROM ... WHERE ...);  


5. CONTROL FLOW / SCRIPTING  
Use Databricks SQL scripting syntax (ANSI/PSM) as follows inside the procedure body (within BEGIN … END):  

    -- IF / ELSE / ELSEIF  
    IF condition THEN  
        …;  
    ELSEIF other_condition THEN  
        …;  
    ELSE  
        …;  
    END IF;  

    -- CASE  
    CASE  
        WHEN condition1 THEN result1  
        WHEN condition2 THEN result2  
        ELSE default_result  
    END CASE;  

    -- WHILE loop  
     label :  WHILE condition DO  
        …;  
    END WHILE  label ;  

    -- LOOP (infinite / generic loop)  
     label :  LOOP  
        …;  
    END LOOP  label ;  

    -- FOR loop (row‑by‑row over query result)  
     label :  FOR [ variable_name AS ] query  
    DO  
        …;  
    END FOR  label ;  

    -- REPEAT loop (executes at least once, repeats until condition true)  
     label : ]REPEAT  
        …;  
    UNTIL condition  
    END REPEAT  label ;  

    -- Loop control inside loops  
    ITERATE label;   -- to skip to next iteration of the loop  
    LEAVE label;     -- to exit the loop  

    -- Condition / Exception Handling (optional)  
    DECLARE HANDLER ...  -- to catch errors and optionally SIGNAL or RESIGNAL  

All statements must end with semicolons. Nesting of conditionals and loops is allowed. 
Use Label for FOR & WHILE Loop. 


6. TEMP TABLES / TABLE VARIABLES  
Convert temp tables (#temp) or table variables (@tbl) into either:  
- WITH clause / CTEs; or  
- regular (temporary) views or tables; or  
- inline queries — whichever best preserves original logic.  

7. TRANSACTIONS  
Be aware that multi‑statement transactions (BEGIN TRANSACTION / COMMIT / ROLLBACK) in T‑SQL may not map directly; Databricks SQL may not fully support them.  
Keep usage minimal or refactor logic to avoid multi‑statement transactional dependencies.  

8. KEYWORD CAPITALIZATION  
ALL SQL KEYWORDS MUST BE UPPER CASE.  
Identifiers (table names, column names, variables, parameters) must preserve original case.  

9. PRESERVE SEMANTICS  
Do NOT change business logic, join/filter logic, or table/column names.  
Do NOT introduce new functionality unless strictly required by mapping rules.  
Do NOT remove or change logic just for syntax convenience (unless impossible in Databricks — in which case raise an error or require manual intervention).  
"""

# --------------------------------------
# STOP REASON HELPERS
# --------------------------------------

def extract_stop_reason(resp_json: dict) -> Optional[str]:
    try:
        choices = resp_json.get("choices")
        if choices:
            r = choices[0].get("stop_reason") or choices[0].get("finish_reason")
            if r:
                return r
        for k in ("stop_reason", "finish_reason", "stop"):
            if k in resp_json:
                return resp_json[k]
        return None
    except Exception:
        return None


def is_token_limit(stop_reason: Optional[str]) -> bool:
    if not stop_reason:
        return False
    s = stop_reason.lower()
    return any(t in s for t in ["max", "length", "limit", "token"])


# --------------------------------------
# CORE SIMPLE LLM CALLER WITH RESUME
# --------------------------------------

def call_llm_until_complete(base_url: str, token: str, model: str, user_prompt: str, max_tokens: int) -> str:
    """
    Sends full input to the LLM. If the model stops for token/length reason,
    take the partial output and re-send: FULL_INPUT + PARTIAL_OUTPUT
    until normal stop.
    """

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    accumulated = ""
    current_prompt = user_prompt

    while True:
        payload = {
            "model": model,
            "model": model,
            "messages": [
                {"role": "user", "content": current_prompt}
            ],
            "max_tokens": max_tokens
        }

        resp = requests.post(base_url, headers=headers, data=json.dumps(payload))
        if resp.status_code != 200:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")

        data = resp.json()
        with open("llm_raw_response.json", "w", encoding="utf-8") as f:
            f.write(json.dumps(data, indent=2))
        # Extract output text (OpenAI/Claude shape)
        output = None
        if "choices" in data and data["choices"]:
            msg = data["choices"][0].get("message") or {}
            output = msg.get("content")

        if not output:
            raise RuntimeError("No text returned by LLM")

        accumulated += output
        stop_reason = extract_stop_reason(data)

        if stop_reason and not is_token_limit(stop_reason):
            # finished normally
            logger.info(f"Finished normally: stop_reason={stop_reason}")
            break

        if stop_reason and is_token_limit(stop_reason):
            logger.info("Token limit reached; continuing...")
            # Build continuation prompt: full input + everything generated so far
            current_prompt = user_prompt + "\n\nPartial output so far:\n" + accumulated + "\n\nContinue the output from where it stopped and dont add any extra comments ."
            time.sleep(0.3)
            continue

        # Unknown stop reason → assume done
        break

    return accumulated


# --------------------------------------
# MAIN SIMPLE FUNCTION
# --------------------------------------

def convert_sql_file(base_url: str, token: str, model: str, sql_path: str, user_prompt: str, max_tokens: int = 5000) -> str:
    sql_text = open(sql_path, "r").read()

    full_prompt = f"{user_prompt} {sql_text}"

    final_output = call_llm_until_complete(
        base_url=base_url,
        token=token,
        model=model,
        user_prompt=full_prompt,
        max_tokens=max_tokens
    )

    return final_output

final_result = convert_sql_file(
    base_url="https://cloud.databricks.com/serving-endpoints/databricks-claude-sonnet-4-5/invocations",
    model="databricks-claude-sonnet-4-5",
    token="",
    user_prompt=IMPROVED_PROMPT,
    max_tokens=8192,
    sql_path=r"test.sql"
)


formatted = sqlparse.format(
    final_result,
    reindent=True,
    keyword_case="upper",
    identifier_case=None
)

# Write formatted SQL to file
with open("final_combined_output.sql", "w", encoding="utf-8") as f:
    f.write(formatted)
