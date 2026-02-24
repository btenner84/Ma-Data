"""
AI Query Service

Translates natural language questions into SQL queries using the semantic model.
All queries are audited for lineage tracking.

Features:
1. Natural language to SQL translation
2. Entity/measure/dimension recognition
3. Query validation against semantic model
4. Full audit trail from question to result to source files
"""

import os
import sys
import json
import yaml
import re
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime

# Add parent directory for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from db import get_engine

# Load semantic model
SEMANTIC_MODEL_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'config',
    'semantic_model.yaml'
)


class AIQueryService:
    """
    AI-powered query service for natural language data access.

    Uses semantic model to understand user intent and generate appropriate SQL.
    """

    def __init__(self):
        self.engine = get_engine()
        self.semantic_model = self._load_semantic_model()

    def _load_semantic_model(self) -> Dict:
        """Load semantic model configuration."""
        try:
            with open(SEMANTIC_MODEL_PATH, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"Warning: Could not load semantic model: {e}")
            return {}

    def parse_question(self, question: str) -> Dict:
        """
        Parse a natural language question to identify entities, measures, dimensions.

        Returns a structured representation of the query intent.
        """
        question_lower = question.lower()

        parsed = {
            'original_question': question,
            'entities': [],
            'measures': [],
            'dimensions': [],
            'filters': {},
            'aggregation': None,
            'time_range': None,
            'suggested_table': None,
            'warnings': []
        }

        # Identify entities (payers, plans)
        entities = self.semantic_model.get('entities', {})
        for entity_name, entity_def in entities.items():
            aliases = entity_def.get('aliases', [])
            for alias in [entity_name] + aliases:
                if alias.lower() in question_lower:
                    parsed['entities'].append({
                        'type': entity_name,
                        'matched_alias': alias,
                        'table': entity_def.get('table'),
                        'examples': entity_def.get('examples', [])
                    })
                    break

        # Identify measures (enrollment, market share, etc.)
        measures = self.semantic_model.get('measures', {})
        for measure_name, measure_def in measures.items():
            aliases = measure_def.get('aliases', [])
            for alias in [measure_name] + aliases:
                if alias.lower() in question_lower:
                    parsed['measures'].append({
                        'name': measure_name,
                        'matched_alias': alias,
                        'column': measure_def.get('column'),
                        'calculation': measure_def.get('calculation'),
                        'fact_table': measure_def.get('fact_table')
                    })
                    break

        # Identify dimensions (plan type, state, etc.)
        dimensions = self.semantic_model.get('dimensions', {})
        for dim_name, dim_def in dimensions.items():
            aliases = dim_def.get('aliases', [])
            for alias in [dim_name] + aliases:
                if alias.lower() in question_lower:
                    parsed['dimensions'].append({
                        'name': dim_name,
                        'matched_alias': alias,
                        'column': dim_def.get('column'),
                        'values': dim_def.get('values', {})
                    })
                    break

            # Check for specific dimension values
            if 'values' in dim_def:
                for value, description in dim_def['values'].items():
                    if value.lower() in question_lower:
                        parsed['filters'][dim_def.get('column', dim_name)] = value

        # Identify time references
        year_match = re.search(r'\b(20\d{2})\b', question)
        if year_match:
            parsed['time_range'] = {'year': int(year_match.group(1))}

        # Check for trend/time series queries
        trend_keywords = ['trend', 'over time', 'growth', 'change', 'year over year', 'yoy']
        if any(kw in question_lower for kw in trend_keywords):
            parsed['aggregation'] = 'timeseries'

        # Check for ranking/top N queries
        top_match = re.search(r'top\s+(\d+)', question_lower)
        if top_match:
            parsed['aggregation'] = 'ranking'
            parsed['filters']['limit'] = int(top_match.group(1))

        # Determine suggested table based on query routing
        parsed['suggested_table'] = self._suggest_table(parsed, question_lower)

        # Add warnings for constraints
        parsed['warnings'] = self._check_constraints(parsed)

        return parsed

    def _suggest_table(self, parsed: Dict, question_lower: str) -> str:
        """Suggest the best table based on query patterns."""
        routing = self.semantic_model.get('query_routing', [])

        for route in routing:
            patterns = route.get('pattern', [])
            if any(p.lower() in question_lower for p in patterns):
                return route.get('use_table', 'fact_enrollment_unified')

        # Default based on dimensions present
        if any(d['name'] in ['state', 'county'] for d in parsed['dimensions']):
            return 'fact_enrollment_geographic'

        return 'fact_enrollment_unified'

    def _check_constraints(self, parsed: Dict) -> List[str]:
        """Check for data constraints that should be communicated."""
        warnings = []
        constraints = self.semantic_model.get('constraints', [])

        for constraint in constraints:
            applies_to = constraint.get('applies_to', [])

            # Check if constraint applies to any parsed elements
            for dim in parsed['dimensions']:
                if dim['name'] in applies_to or dim.get('column') in applies_to:
                    warnings.append(constraint.get('user_message', constraint.get('description')))

            if parsed['suggested_table'] in applies_to:
                warnings.append(constraint.get('user_message', constraint.get('description')))

        return list(set(warnings))  # Deduplicate

    def generate_sql(self, parsed: Dict) -> str:
        """
        Generate SQL from parsed question structure.

        This is a simplified generator - for production, you'd want
        to use an LLM to handle more complex queries.
        """
        table = parsed['suggested_table'] or 'fact_enrollment_unified'

        # Build SELECT clause
        select_cols = []
        group_cols = []

        # Add dimension columns
        for dim in parsed['dimensions']:
            col = dim.get('column', dim['name'])
            select_cols.append(col)
            group_cols.append(col)

        # Add measure columns with aggregation
        for measure in parsed['measures']:
            calc = measure.get('calculation', f"SUM({measure.get('column', 'enrollment')})")
            select_cols.append(f"{calc} as {measure['name']}")

        # Default to enrollment if no measures specified
        if not parsed['measures']:
            select_cols.append("SUM(enrollment) as enrollment")

        # Add year for time series
        if parsed['aggregation'] == 'timeseries':
            if 'year' not in [d['name'] for d in parsed['dimensions']]:
                select_cols.insert(0, 'year')
                group_cols.insert(0, 'year')

        # Build WHERE clause
        where_clauses = []

        # Add time filter
        if parsed['time_range']:
            if 'year' in parsed['time_range']:
                if parsed['aggregation'] != 'timeseries':
                    where_clauses.append(f"year = {parsed['time_range']['year']}")

        # Add dimension filters
        for col, value in parsed['filters'].items():
            if col != 'limit':
                where_clauses.append(f"{col} = '{value}'")

        # Default to January for month
        where_clauses.append("month = 1")

        # Build SQL
        sql = f"SELECT {', '.join(select_cols)}\nFROM {table}"

        if where_clauses:
            sql += f"\nWHERE {' AND '.join(where_clauses)}"

        if group_cols:
            sql += f"\nGROUP BY {', '.join(group_cols)}"

        # Add ORDER BY
        if parsed['aggregation'] == 'timeseries':
            sql += "\nORDER BY year"
        elif parsed['aggregation'] == 'ranking':
            sql += "\nORDER BY enrollment DESC"
        else:
            sql += "\nORDER BY enrollment DESC"

        # Add LIMIT
        if 'limit' in parsed['filters']:
            sql += f"\nLIMIT {parsed['filters']['limit']}"
        elif parsed['aggregation'] == 'ranking':
            sql += "\nLIMIT 10"

        return sql

    def query(
        self,
        question: str,
        user_id: str = "ai_query",
        execute: bool = True
    ) -> Dict:
        """
        Process a natural language question.

        Args:
            question: Natural language question
            user_id: User identifier for audit
            execute: If True, execute the query and return results

        Returns:
            Dict with parsed question, generated SQL, and optionally results
        """
        # Parse the question
        parsed = self.parse_question(question)

        # Generate SQL
        sql = self.generate_sql(parsed)

        result = {
            'question': question,
            'parsed': parsed,
            'sql': sql,
            'warnings': parsed['warnings']
        }

        if execute:
            try:
                df, audit_id = self.engine.query_with_audit(
                    sql,
                    user_id=user_id,
                    context=f"AI query: {question[:100]}"
                )

                result['data'] = df.to_dict(orient='records')
                result['row_count'] = len(df)
                result['audit_id'] = audit_id
                result['status'] = 'success'

            except Exception as e:
                result['error'] = str(e)
                result['status'] = 'error'

        return result

    def get_suggestions(self, partial_question: str) -> List[str]:
        """
        Get question suggestions based on partial input.

        Useful for autocomplete in UI.
        """
        suggestions = []

        # Get example queries from semantic model
        examples = self.semantic_model.get('example_queries', [])
        for ex in examples:
            q = ex.get('question', '')
            if partial_question.lower() in q.lower():
                suggestions.append(q)

        # Add template suggestions
        templates = [
            "What is the total MA enrollment in {year}?",
            "Who are the top 10 payers by enrollment?",
            "What is the D-SNP enrollment trend?",
            "What is {payer}'s market share?",
            "Which states have the highest MA enrollment?",
            "What is the enrollment by plan type?",
            "How has enrollment grown over time?",
            "What percentage of enrollment is Individual vs Group?",
            "What is the average star rating for 4+ star plans?",
            "How does HMO enrollment compare to PPO?",
        ]

        for template in templates:
            if partial_question.lower() in template.lower():
                suggestions.append(template)

        return suggestions[:10]  # Limit to 10 suggestions

    def explain_query(self, question: str) -> Dict:
        """
        Explain how a question would be interpreted.

        Useful for debugging and user education.
        """
        parsed = self.parse_question(question)
        sql = self.generate_sql(parsed)

        return {
            'question': question,
            'interpretation': {
                'entities_found': [e['type'] for e in parsed['entities']],
                'measures_found': [m['name'] for m in parsed['measures']],
                'dimensions_found': [d['name'] for d in parsed['dimensions']],
                'filters_applied': parsed['filters'],
                'time_range': parsed['time_range'],
                'query_type': parsed['aggregation'] or 'simple',
            },
            'table_selected': parsed['suggested_table'],
            'sql_generated': sql,
            'warnings': parsed['warnings'],
            'data_lineage': {
                'table': parsed['suggested_table'],
                'source_files': self._get_table_sources(parsed['suggested_table'])
            }
        }

    def _get_table_sources(self, table_name: str) -> List[str]:
        """Get source files for a table."""
        sources = {
            'fact_enrollment_unified': [
                'CMS Monthly Enrollment by Plan',
                'CMS CPSC (Contract Info)',
                'CMS SNP Comprehensive Report'
            ],
            'fact_enrollment_geographic': [
                'CMS CPSC (Enrollment Info)'
            ],
            'fact_star_ratings': [
                'CMS Medicare Star Ratings'
            ],
            'fact_risk_scores': [
                'CMS Plan Payment Data'
            ],
            'agg_by_parent_year': [
                'Derived from fact_enrollment_unified'
            ],
            'agg_by_state_year': [
                'Derived from fact_enrollment_geographic'
            ],
        }
        return sources.get(table_name, ['Unknown'])

    def trace_lineage(self, audit_id: str) -> Dict:
        """
        Full lineage trace from question to source files.

        Shows complete path: Question -> SQL -> Tables -> Source Files
        """
        return self.engine.trace_query_lineage(audit_id)


# Singleton instance
_service_instance = None

def get_ai_query_service() -> AIQueryService:
    """Get or create singleton AI query service."""
    global _service_instance
    if _service_instance is None:
        _service_instance = AIQueryService()
    return _service_instance
