"""
LLM Client: Text-to-SQL Generator with Pluggable Multi-Provider Support
Supports OpenAI, Anthropic Claude, Google Gemini, and an offline rule-based dry-run engine.
"""

import os
import re
import sys
from typing import Tuple, Optional
from schema_context import get_schema_context_prompt


class TextToSQLClient:
    def __init__(self, provider: str = "auto"):
        self.provider = provider
        self.active_provider = "offline-rule-engine"
        self._init_provider()

    def _init_provider(self):
        openai_key = os.getenv("OPENAI_API_KEY")
        anthropic_key = os.getenv("ANTHROPIC_API_KEY")
        gemini_key = os.getenv("GEMINI_API_KEY")

        if self.provider == "openai" or (self.provider == "auto" and openai_key):
            try:
                from openai import OpenAI
                self.openai_client = OpenAI(api_key=openai_key)
                self.active_provider = "openai"
                print("[INFO] Text-to-SQL initialized with OpenAI provider.")
                return
            except Exception as e:
                print(f"[WARN] Failed to initialize OpenAI provider: {e}")

        if self.provider == "anthropic" or (self.provider == "auto" and anthropic_key):
            try:
                import anthropic
                self.anthropic_client = anthropic.Anthropic(api_key=anthropic_key)
                self.active_provider = "anthropic"
                print("[INFO] Text-to-SQL initialized with Anthropic Claude provider.")
                return
            except Exception as e:
                print(f"[WARN] Failed to initialize Anthropic provider: {e}")

        if self.provider == "gemini" or (self.provider == "auto" and gemini_key):
            try:
                import google.generativeai as genai
                genai.configure(api_key=gemini_key)
                self.gemini_model = genai.GenerativeModel("gemini-1.5-flash")
                self.active_provider = "gemini"
                print("[INFO] Text-to-SQL initialized with Google Gemini provider.")
                return
            except Exception as e:
                print(f"[WARN] Failed to initialize Gemini provider: {e}")

        self.active_provider = "offline-rule-engine"
        print("[INFO] No external LLM key provided. Text-to-SQL initialized with deterministic offline query engine.")

    def generate_sql(self, question: str) -> Tuple[str, str]:
        """
        Translates a natural language question into PostgreSQL SQL.
        Returns: (sql_query, provider_name)
        """
        if self.active_provider == "openai":
            sql = self._generate_with_openai(question)
        elif self.active_provider == "anthropic":
            sql = self._generate_with_anthropic(question)
        elif self.active_provider == "gemini":
            sql = self._generate_with_gemini(question)
        else:
            sql = self._generate_with_offline_engine(question)

        cleaned_sql = self._clean_and_validate_sql(sql)
        return cleaned_sql, self.active_provider

    def _get_system_prompt(self) -> str:
        return f"""You are a Senior Data Engineer and PostgreSQL SQL expert.
Convert the user's natural language question into a clean, executable, standard PostgreSQL SELECT query against the analytics data marts.

{get_schema_context_prompt()}

CRITICAL CONSTRAINTS:
1. Output ONLY the raw SQL query. Do not wrap in markdown quotes if possible, or use standard ```sql block.
2. No commentary, no explanations, no prologue, no epilogue.
3. Use only read-only SELECT or WITH statements.
4. Always qualify tables with 'analytics.' schema.
5. Limit results to 50 rows unless asked otherwise.
"""

    def _generate_with_openai(self, question: str) -> str:
        response = self.openai_client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0.0,
            messages=[
                {"role": "system", "content": self._get_system_prompt()},
                {"role": "user", "content": question}
            ]
        )
        return response.choices[0].message.content.strip()

    def _generate_with_anthropic(self, question: str) -> str:
        response = self.anthropic_client.messages.create(
            model="claude-3-haiku-20240307",
            max_tokens=500,
            temperature=0.0,
            system=self._get_system_prompt(),
            messages=[{"role": "user", "content": question}]
        )
        return response.content[0].text.strip()

    def _generate_with_gemini(self, question: str) -> str:
        prompt = f"{self._get_system_prompt()}\n\nUser Question: {question}\nSQL Output:"
        response = self.gemini_model.generate_content(prompt)
        return response.text.strip()

    def _generate_with_offline_engine(self, question: str) -> str:
        """
        High-precision deterministic SQL generator for standard analytics query patterns.
        Enables 100% reliable local testing and interview demos without external cloud dependencies.
        """
        q = question.lower().strip()

        # Category sales / revenue breakdown
        if any(k in q for k in ["category", "categories"]):
            return """
SELECT 
    p.category,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS total_units_sold,
    ROUND(SUM(f.gross_amount), 2) AS total_revenue
FROM analytics.fct_orders f
JOIN analytics.dim_products p ON f.product_id = p.product_id
WHERE f.is_completed = 1
GROUP BY p.category
ORDER BY total_revenue DESC;
""".strip()

        # Top customers by spend / revenue / orders
        if any(k in q for k in ["top customer", "top 5 customer", "top 10 customer", "top spend", "best customer", "highest spend", "customer tier"]):
            limit_match = re.search(r"top\s+(\d+)", q)
            limit_val = limit_match.group(1) if limit_match else "5"
            return f"""
SELECT 
    customer_id,
    customer_name,
    customer_tier,
    completed_orders,
    lifetime_value,
    total_items_purchased
FROM analytics.dim_customers
ORDER BY lifetime_value DESC
LIMIT {limit_val};
""".strip()

        # Top selling products
        if any(k in q for k in ["top product", "best selling", "top 5 product", "top 10 product", "popular product", "most sold"]):
            limit_match = re.search(r"top\s+(\d+)", q)
            limit_val = limit_match.group(1) if limit_match else "5"
            return f"""
SELECT 
    product_id,
    product_name,
    category,
    current_unit_price,
    total_units_sold,
    total_revenue
FROM analytics.dim_products
ORDER BY total_revenue DESC
LIMIT {limit_val};
""".strip()

        # Regional breakdown / geographic distribution
        if any(k in q for k in ["region", "regional", "geography", "territory", "apac", "eu", "na", "latam"]):
            return """
SELECT 
    region,
    COUNT(DISTINCT order_id) AS order_count,
    SUM(quantity) AS units_sold,
    ROUND(SUM(gross_amount), 2) AS total_revenue,
    ROUND(AVG(gross_amount), 2) AS avg_order_value
FROM analytics.fct_orders
WHERE is_completed = 1
GROUP BY region
ORDER BY total_revenue DESC;
""".strip()

        # Daily trends / recent volume / timeline
        if any(k in q for k in ["daily", "day", "trend", "volume", "last 7 days", "date", "recent", "timeline", "time"]):
            return """
SELECT 
    order_date,
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(CASE WHEN is_completed = 1 THEN quantity ELSE 0 END) AS completed_units,
    ROUND(SUM(CASE WHEN is_completed = 1 THEN gross_amount ELSE 0 END), 2) AS daily_revenue
FROM analytics.fct_orders
GROUP BY order_date
ORDER BY order_date DESC
LIMIT 14;
""".strip()

        # Order status distribution / cancellation rates
        if any(k in q for k in ["status", "cancel", "return", "pending", "completion rate"]):
            return """
SELECT 
    order_status,
    COUNT(order_id) AS order_count,
    ROUND(COUNT(order_id) * 100.0 / SUM(COUNT(order_id)) OVER (), 1) AS percentage_of_total,
    ROUND(SUM(gross_amount), 2) AS total_gross_value
FROM analytics.fct_orders
GROUP BY order_status
ORDER BY order_count DESC;
""".strip()

        # Default fallback summary query
        return """
SELECT 
    f.order_date,
    p.category,
    f.region,
    COUNT(DISTINCT f.order_id) AS order_count,
    ROUND(SUM(f.gross_amount), 2) AS total_revenue
FROM analytics.fct_orders f
JOIN analytics.dim_products p ON f.product_id = p.product_id
WHERE f.is_completed = 1
GROUP BY f.order_date, p.category, f.region
ORDER BY f.order_date DESC, total_revenue DESC
LIMIT 20;
""".strip()

    def _clean_and_validate_sql(self, raw_sql: str) -> str:
        """Strips markdown markers and enforces read-only SQL safety."""
        # Strip markdown ```sql ... ```
        cleaned = re.sub(r"^```(?:sql)?", "", raw_sql.strip(), flags=re.IGNORECASE)
        cleaned = re.sub(r"```$", "", cleaned.strip()).strip()

        # Remove trailing semicolon for query execution flexibility
        cleaned = re.sub(r";\s*$", "", cleaned).strip()

        # Safety validation
        first_token = cleaned.split()[0].upper() if cleaned.split() else ""
        if first_token not in ("SELECT", "WITH"):
            raise ValueError(f"Security Alert: Generated query must be a SELECT or WITH statement. Got: '{first_token}'")

        forbidden = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE", "GRANT", "REVOKE", "EXEC", "EXECUTE"]
        for word in forbidden:
            if re.search(r"\b" + word + r"\b", cleaned, flags=re.IGNORECASE):
                raise ValueError(f"Security Alert: Query contains forbidden keyword '{word}'. Execution blocked.")

        return cleaned
