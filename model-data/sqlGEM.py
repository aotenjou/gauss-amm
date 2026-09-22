import os
import sys
import argparse
import random
import json
import re
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
from datetime import datetime

# --- NEW: A class to tee stdout to both console and a log file ---
class Logger:
    def __init__(self, filepath, original_stdout):
        self.terminal = original_stdout
        self.log_file = open(filepath, 'w', encoding='utf-8')

    def write(self, message):
        self.terminal.write(message)
        self.log_file.write(message)

    def flush(self):
        # This flush method is needed for compatibility with file-like objects.
        self.terminal.flush()
        self.log_file.flush()

    def close(self):
        self.log_file.close()

ROOT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG = ROOT_DIR / "config" / "postgres.json"
DB_CONFIG = {}
PROMPTS = {}
SPECS = []
client = None
MODEL_NAME = ""
MODEL_BASE_URL = ""
MODEL_API_KEY_ENV = "OPENAI_API_KEY"
MODEL_API_ENV_FILE = ""
DB_SCHEMA = "public"

# Logging initialization (for the separate generation.log file)
logging.basicConfig(
    filename="generation.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_resources(config_path=None, spec_path=None, prompt_dir=None):
    """Load database, model, specs and prompts from external files."""
    global DB_CONFIG, PROMPTS, SPECS, client, MODEL_NAME, MODEL_BASE_URL, MODEL_API_KEY_ENV, MODEL_API_ENV_FILE, DB_SCHEMA
    client = None
    config_file = Path(config_path or DEFAULT_CONFIG)
    config = json.loads(config_file.read_text(encoding="utf-8"))
    DB_CONFIG = dict(config.get("database", {}))
    # ``schema`` controls search_path/schema introspection and is not a
    # psycopg2 connection keyword.
    DB_CONFIG.pop("schema", None)
    DB_CONFIG = {k: v for k, v in DB_CONFIG.items() if v is not None and v != ""}
    model = config.get("model", {})
    MODEL_API_KEY_ENV = model.get("api_key_env", "OPENAI_API_KEY")
    MODEL_API_ENV_FILE = str(config.get("api_env_file", ""))
    if MODEL_API_ENV_FILE:
        env_file = Path(MODEL_API_ENV_FILE)
        if not env_file.is_absolute():
            env_file = config_file.parent / env_file
        if env_file.exists() and not os.getenv(MODEL_API_KEY_ENV):
            raw_key = env_file.read_text(encoding="utf-8").strip()
            first_line = raw_key.splitlines()[0] if raw_key else ""
            if "=" in first_line:
                key_name, raw_key = first_line.split("=", 1)
                MODEL_API_KEY_ENV = key_name.strip()
                raw_key = raw_key.strip().strip("'\"")
            if raw_key:
                os.environ[MODEL_API_KEY_ENV] = raw_key
    MODEL_BASE_URL = model.get("base_url", "https://api.deepseek.com")
    MODEL_NAME = model.get("name", "deepseek-chat")
    DB_SCHEMA = str(config.get("database", {}).get("schema", "public"))
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", DB_SCHEMA):
        raise ValueError(f"unsafe database schema identifier: {DB_SCHEMA!r}")
    spec_file = Path(spec_path or config.get("spec_file", ROOT_DIR / "specs.json"))
    if not spec_file.is_absolute():
        spec_file = config_file.parent / spec_file
    SPECS = json.loads(spec_file.read_text(encoding="utf-8"))
    prompt_path = Path(prompt_dir or config.get("prompt_dir", ROOT_DIR / "prompts"))
    if not prompt_path.is_absolute():
        prompt_path = config_file.parent / prompt_path
    PROMPTS = {
        "generation": (prompt_path / "generation.txt").read_text(encoding="utf-8"),
        "fix": (prompt_path / "fix.txt").read_text(encoding="utf-8"),
    }


def render_prompt(template, **values):
    """Replace only named resource tokens, preserving SQL's curly braces."""
    for name, value in values.items():
        template = template.replace("{{" + name.upper() + "}}", str(value))
    return template


def get_llm_client():
    """Create the API client only when a generation request is made."""
    global client
    if client is None:
        api_key = os.getenv(MODEL_API_KEY_ENV, "")
        if not api_key:
            raise RuntimeError(
                f"API key is missing; set the {MODEL_API_KEY_ENV} environment variable before generating SQL"
            )
        try:
            from openai import OpenAI
        except ImportError as exc:
            raise RuntimeError(
                "The OpenAI SDK is required for generation; install it with: pip install openai"
            ) from exc
        client = OpenAI(api_key=api_key, base_url=MODEL_BASE_URL)
    return client


# ========== Prompt Construction ==========
def build_generation_prompt(schema_excerpt, user_spec):
    """
    Build the prompt for SQL template generation.
    """
    return render_prompt(PROMPTS["generation"], schema=schema_excerpt,
                         feedback="", requirement=user_spec)

def build_fix_prompt(schema_excerpt, user_spec, bad_sql, err):
    """
    Build the prompt for fixing SQL templates that failed validation.
    """
    return render_prompt(PROMPTS["fix"], error=err, sql=bad_sql,
                         schema=schema_excerpt, requirement=user_spec)

# ========== Database Utilities ==========
def get_schema_excerpt():
    """
    Extract database schema information from PostgreSQL.
    """
    print("Fetching database schema information...")
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema=%s ORDER BY table_name;""", (DB_SCHEMA,))
        tables = [r[0] for r in cur.fetchall()]
        print(f"Found {len(tables)} tables: {', '.join(tables)}")

        excerpt = {}
        for i, t in enumerate(tables, 1):
            print(f"  Processing table {i}/{len(tables)}: {t}")
            cur.execute("""SELECT column_name, data_type FROM information_schema.columns WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position;""", (DB_SCHEMA, t))
            excerpt[f"{DB_SCHEMA}.{t}"] = cur.fetchall()

        print("✅ Schema information retrieved successfully")
        return json.dumps(excerpt, indent=2)
    except Exception as e:
        print(f"❌ Failed to fetch schema: {e}")
        logging.error(f"Schema extraction failed: {e}")
        raise
    finally:
        if conn is not None:
            conn.close()

def validate_query(sql):
    """
    Execute EXPLAIN to verify that a query is valid for the target database.
    """
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute('SET search_path TO "' + DB_SCHEMA.replace('"', '""') + '", public')
        cur.execute(f"EXPLAIN {sql}")
        plan = cur.fetchall()
        return bool(plan)
    except Exception as e:
        return str(e)
    finally:
        if conn is not None:
            conn.close()
    return None

def parse_and_clean_llm_output(text):
    """
    Uses regex to reliably extract SQL and JSON, and cleans the SQL.
    """
    text = text.strip()
    json_match = re.search(r'{\s*"placeholders":\s*\[.*\]\s*}', text, re.DOTALL)
    if not json_match:
        return None, None

    json_part = json_match.group(0)
    sql_part = text[:json_match.start()].strip()

    sql_part = re.sub(r'^```[a-zA-Z]*\n', '', sql_part)
    sql_part = re.sub(r'\n```$', '', sql_part)
    if sql_part.startswith('"') and sql_part.endswith('"'):
        sql_part = sql_part[1:-1]

    return sql_part.strip(), json_part.strip()

# ========== GPT API Calls ==========
def generate_sql_template(schema_excerpt, user_spec, max_retries=3):
    """
    Generate an SQL template using the configured model and specification.
    """
    for attempt in range(max_retries):
        try:
            prompt = build_generation_prompt(schema_excerpt, user_spec)
            response = get_llm_client().chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "system", "content": "You generate OLAP SQL templates with placeholders."},
                    {"role": "user", "content": prompt}
                ],
                top_p=0.7,
                temperature=0.6
            )
            text = response.choices[0].message.content
            sql, json_part = parse_and_clean_llm_output(text)

            if sql and json_part:
                return sql, json_part
            else:
                print(f"⚠️  Could not parse SQL and JSON from response, retrying... (Spec: {user_spec})")

        except Exception as e:
            print(f"❌ GPT API call failed (Attempt {attempt+1}/{max_retries}): {e}")
            logging.error(f"GPT call failed for spec '{user_spec}': {e}")

    print(f"❌ Template generation failed for spec: {user_spec}")
    return None, None

# ========== Query Instantiation ==========
def instantiate_query(template, placeholders):
    """
    Replace placeholders in an SQL template with actual values.
    """
    query = template
    for ph in placeholders:
        try:
            val_unquoted, final_val = None, ''
            if ph["type"] in ["integer", "bigint", "numeric", "decimal", "float"]:
                min_val, max_val = float(ph["min"]), float(ph["max"])
                if ph["type"] in ["integer", "bigint"]:
                    val_unquoted = random.randint(int(min_val), int(max_val))
                else:
                    val_unquoted = round(random.uniform(min_val, max_val), 2)
                final_val = str(val_unquoted)
            elif ph["type"] == "date":
                val_unquoted = ph['min']
                final_val = f"'{val_unquoted}'"
            else:
                val_unquoted = ph['min']
                final_val = f"'{val_unquoted}'"

            # A range predicate can reference one placeholder multiple times.
            # Reuse the same sampled value throughout that template.
            query = query.replace('{{' + ph['name'] + '}}', final_val)

        except (KeyError, ValueError) as e:
            print(f"⚠️ Error instantiating placeholder '{ph.get('name', 'N/A')}': {e} - Skipping")
            continue
    return query

# ========== Main Process ==========
def run_generation_process(args):
    """
    Main function to orchestrate the SQL generation process.
    This contains the logic previously in main().
    """
    print("🚀 SQL Query Generator Starting")
    print(f"Configuration: target_count={args.target_count}, max_specs={args.max_specs}, templates_per_spec={args.templates_per_spec}, instances_per_template={args.instances_per_template}")

    schema_excerpt = get_schema_excerpt()
    if args.schema_only:
        print("Schema extraction completed; no LLM request was made.")
        return

    all_queries = []

    selected_specs = list(dict.fromkeys(SPECS))[:args.max_specs]
    print(f"\nWill generate templates for the following {len(selected_specs)} specifications:")
    for i, spec in enumerate(selected_specs, 1):
        print(f"  {i}. {spec}")

    for i in range(args.templates_per_spec):
        print("\n" + "="*50)
        print(f"🎬 Starting Generation Round {i+1}/{args.templates_per_spec}")
        print("="*50)

        with ThreadPoolExecutor(max_workers=args.llm_workers) as executor:
            future_to_spec = {executor.submit(generate_sql_template, schema_excerpt, spec): spec for spec in selected_specs}
            round_templates = 0
            for future in as_completed(future_to_spec):
                spec = future_to_spec[future]
                try:
                    sql, json_str = future.result()
                    if sql and json_str:
                        round_templates += 1
                        print(f"\n- Processing template (Spec: {spec})")
                        try:
                            placeholders = json.loads(json_str)["placeholders"]
                            template_queries = 0
                            for _ in range(args.instances_per_template):
                                q = instantiate_query(sql, placeholders)
                                valid = validate_query(q)
                                if valid is True:
                                    all_queries.append(q)
                                    template_queries += 1
                                else:
                                    print(f"  ⚠️ EXPLAIN failed: {valid}")
                                    break
                            if template_queries > 0:
                                print(f"  ✅ Successfully generated {template_queries}/{args.instances_per_template} query instances")
                            else:
                                print(f"  ❌ All instances for this template failed")
                        except json.JSONDecodeError as e:
                            print(f"  ❌ JSON parsing failed: {e}")
                            logging.error(f"JSON parse error for spec '{spec}': {e} | JSON string: {json_str}")
                except Exception as e:
                    print(f"❌ Error processing template for spec '{spec}': {e}")
        print(f"\n🏁 Round {i+1} finished: Successfully processed {round_templates}/{len(selected_specs)} new templates.")
        print(f"   Current total queries: {len(all_queries)}")
        if len(all_queries) >= args.target_count:
            print(f"   Target reached after round {i+1}; skipping remaining rounds.")
            break

    print("\n" + "="*50)
    print("✅ All generation tasks completed.")
    print(f"   Total queries generated: {len(all_queries)}")
    if not all_queries:
        print("❌ No valid queries were generated. Exiting."); return
    print(f"\nSelecting the first {args.target_count} EXPLAIN-valid queries in generation order...")
    selected = all_queries[:args.target_count]
    if not selected:
        print("❌ No queries remaining after sorting and selection. Exiting."); return
    if args.strict_target and len(selected) < args.target_count:
        raise RuntimeError(
            f"strict target not reached: generated {len(selected)} valid SQL statements, "
            f"required {args.target_count}"
        )

    print(f"✅ Final selection: {len(selected)} queries")

    print(f"\nSaving results to {args.output_dir}...")
    # Directory is already created by the main wrapper function
    sql_path = os.path.join(args.output_dir, "generated.sql")
    with open(sql_path, "w", encoding='utf-8') as f:
        f.write(f"-- Generated {len(selected)} SQL queries\n-- Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        for i, q in enumerate(selected, 1):
            statement = q.rstrip()
            while statement.endswith(";"):
                statement = statement[:-1].rstrip()
            f.write(f"-- Query {i}\n{statement};\n\n")
    print(f"   ✅ SQL queries saved to: {sql_path}")

    print("\n🎉 Task Completed Successfully!")
    print(f"   Output directory: {args.output_dir}\n   Final query count: {len(selected)}\n   Detailed logs available in: generation.log")
    print(f"   Full console output saved to: {os.path.join(args.output_dir, 'gen_log.txt')}")

def main():
    """
    Main entry point: parses args, sets up logging, and calls the generation process.
    """
    parser = argparse.ArgumentParser(description="Database SQL generator using an LLM and an EXPLAIN feedback loop")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="JSON file containing database/model/resource configuration.")
    parser.add_argument("--spec-file", help="Override the external JSON specification file.")
    parser.add_argument("--prompt-dir", help="Override the directory containing generation.txt and fix.txt.")
    parser.add_argument("--schema-only", action="store_true", help="Extract and print the schema without calling the LLM.")
    parser.add_argument("--schema", help="Database schema containing benchmark tables.")
    parser.add_argument("--database", help="Override the database name from the selected config.")
    parser.add_argument("--strict-target", action="store_true", help="Fail unless target-count EXPLAIN-valid SQL statements are produced.")
    parser.add_argument("--target-count", type=int, default=50, help="Target number of queries to finally select.")
    parser.add_argument("--max-specs", type=int, default=10, help="Maximum number of unique query specifications to use.")
    parser.add_argument("--templates-per-spec", type=int, default=5, help="Number of unique templates per spec (feedback rounds).")
    parser.add_argument("--instances-per-template", type=int, default=5, help="Number of SQL instances per template.")
    parser.add_argument("--llm-workers", type=int, default=4,
                        help="Concurrent DeepSeek template requests (default: 4).")
    parser.add_argument("--output-dir", type=str, default="./output", help="Output directory to save the results.")
    args = parser.parse_args()
    if args.llm_workers < 1:
        parser.error("--llm-workers must be at least 1")

    load_resources(args.config, args.spec_file, args.prompt_dir)
    if args.schema:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", args.schema):
            parser.error("--schema must be a simple SQL identifier")
        DB_SCHEMA = args.schema
    if args.database:
        DB_CONFIG["dbname"] = args.database

    # --- NEW: Setup for stdout redirection ---
    # Ensure the output directory exists before trying to create the log file
    os.makedirs(args.output_dir, exist_ok=True)
    log_filepath = os.path.join(args.output_dir, 'gen_log.txt')

    original_stdout = sys.stdout
    logger = None

    try:
        logger = Logger(log_filepath, original_stdout)
        sys.stdout = logger
        run_generation_process(args)
    except KeyboardInterrupt:
        print("\n\nProgram interrupted by user.")
    except Exception as e:
        print(f"\n❌ A critical error occurred: {e}")
        logging.error(f"Program failed with a critical error.", exc_info=True)
        return 1
    finally:
        # --- NEW: Cleanup to restore stdout and close the file ---
        if logger:
            logger.close()
        sys.stdout = original_stdout
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
