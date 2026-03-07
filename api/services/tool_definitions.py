"""
Tool Definitions for MA Agent V3

Provides LLM-readable tool schemas for function calling.
Each tool definition includes:
- Name and description
- Parameter schema with types, descriptions, and valid values
- Dynamic injection of actual database values (payers, years, etc.)
"""

from typing import Dict, List, Any, Optional
from api.services.enrollment_service import get_enrollment_service
from api.services.stars_service import get_stars_service
from api.services.risk_scores_service import get_risk_scores_service


class ToolDefinitions:
    """
    Generates tool definitions for LLM function calling.
    Dynamically injects valid filter values from the database.
    """
    
    def __init__(self):
        self._cached_filters: Optional[Dict] = None
    
    def _get_filters(self) -> Dict:
        """Get cached filter values from services."""
        if self._cached_filters is None:
            enrollment_svc = get_enrollment_service()
            stars_svc = get_stars_service()
            risk_svc = get_risk_scores_service()
            
            e_filters = enrollment_svc.get_filters()
            s_filters = stars_svc.get_filters()
            r_filters = risk_svc.get_filters()
            
            # Get top 20 payers for prompt (to save tokens)
            top_payers = e_filters.get('parent_orgs', [])[:20]
            
            self._cached_filters = {
                'years': e_filters.get('years', []),
                'star_years': s_filters.get('star_years', []),
                'risk_years': r_filters.get('years', []),
                'top_payers': top_payers,
                'all_payers_count': len(e_filters.get('parent_orgs', [])),
                'plan_types': e_filters.get('plan_types_simplified', []),
                'snp_types': e_filters.get('snp_types', []),
                'states': e_filters.get('states', []),
                'domains': s_filters.get('domains', []),
                'parts': s_filters.get('parts', [])
            }
        return self._cached_filters
    
    def get_tool_definitions(self) -> List[Dict]:
        """
        Get all tool definitions for LLM function calling.
        Returns Anthropic-compatible tool schema format.
        """
        f = self._get_filters()
        
        return [
            # =================================================================
            # ENROLLMENT TOOLS
            # =================================================================
            {
                "name": "get_enrollment_by_payer",
                "description": f"Get enrollment ranking by parent organization for a specific year. Returns top payers with enrollment counts, market share percentages, and contract counts. Use for questions like 'top MA payers', 'who has most enrollment', 'market leaders'. Data years: {min(f['years'])}-{max(f['years'])}.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "year": {
                            "type": "integer",
                            "description": f"Year to query. Range: {min(f['years'])}-{max(f['years'])}. Use {max(f['years'])} for current/latest."
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Number of top payers to return. Default 20.",
                            "default": 20
                        }
                    },
                    "required": ["year"]
                }
            },
            {
                "name": "get_enrollment_timeseries",
                "description": f"Get enrollment timeseries over years with filters. Can filter by payers, plan types, SNP types. Use for 'enrollment over time', 'enrollment trend', 'D-SNP growth'. Top payers include: {', '.join(f['top_payers'][:10])}... (plus {f['all_payers_count'] - 10} more). Plan types: {', '.join(f['plan_types'])}. SNP types: {', '.join(f['snp_types'])}.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "payers": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Parent organizations to filter. Use common names like 'UNH', 'Humana', 'CVS', 'Cigna', 'Kaiser'. System will normalize."
                        },
                        "plan_types": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": f"Plan types to filter. Options: {', '.join(f['plan_types'])}"
                        },
                        "snp_types": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": f"SNP types to filter. Options: {', '.join(f['snp_types'])}. 'D-SNP' = Dual eligible."
                        },
                        "group_types": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Group types: 'Individual', 'Group'"
                        },
                        "start_year": {
                            "type": "integer",
                            "description": f"Start year. Default {min(f['years'])}.",
                            "default": min(f['years'])
                        },
                        "end_year": {
                            "type": "integer",
                            "description": f"End year. Default {max(f['years'])}.",
                            "default": max(f['years'])
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "get_enrollment_by_dimension",
                "description": "Get enrollment breakdown by dimension (plan type, SNP type, product type). Use for 'enrollment by plan type', 'HMO vs PPO', 'how much is D-SNP'.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "year": {
                            "type": "integer",
                            "description": f"Year to query. Range: {min(f['years'])}-{max(f['years'])}."
                        },
                        "plan_type": {
                            "type": "string",
                            "description": f"Filter to specific plan type: {', '.join(f['plan_types'])}"
                        },
                        "snp_type": {
                            "type": "string",
                            "description": f"Filter to SNP type: {', '.join(f['snp_types'])}"
                        }
                    },
                    "required": ["year"]
                }
            },
            {
                "name": "get_enrollment_by_state",
                "description": "Get enrollment by state for a specific year. Use for 'enrollment by state', 'which states have most MA', 'California enrollment'.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "year": {
                            "type": "integer",
                            "description": f"Year to query. Range: {min(f['years'])}-{max(f['years'])}."
                        }
                    },
                    "required": ["year"]
                }
            },
            
            # =================================================================
            # STARS TOOLS
            # =================================================================
            {
                "name": "get_stars_distribution",
                "description": f"Get star rating distribution showing 4+ star enrollment percentage over time. Returns timeseries of what % of each payer's enrollment is in 4+ star plans. Star years: {min(f['star_years'])}-{max(f['star_years'])}. Use for '4+ star percentage', 'star rating trends', 'quality performance'.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "payers": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Parent organizations to filter. Use common names."
                        },
                        "plan_types": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": f"Plan types: {', '.join(f['plan_types'])}"
                        },
                        "snp_types": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": f"SNP types: {', '.join(f['snp_types'])}"
                        },
                        "star_year": {
                            "type": "integer",
                            "description": "Specific year to query (omit for full timeseries)."
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "get_stars_by_payer",
                "description": "Get star ratings by parent organization for a specific year. Returns ranking with enrollment, 4+ star %, weighted average rating. Use for 'payer star rankings', 'who has highest ratings'.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "star_year": {
                            "type": "integer",
                            "description": f"Star year. Range: {min(f['star_years'])}-{max(f['star_years'])}. Default {max(f['star_years'])}.",
                            "default": max(f['star_years'])
                        },
                        "plan_types": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": f"Plan types: {', '.join(f['plan_types'])}"
                        },
                        "snp_types": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": f"SNP types: {', '.join(f['snp_types'])}"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Number of payers to return. Default 50.",
                            "default": 50
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "get_stars_timeseries",
                "description": "Get star rating (4+ star %) timeseries for specific payers. Shows how quality has changed over time. Use for 'star rating history', 'quality trends'.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "payers": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Parent organizations to compare."
                        },
                        "include_industry_total": {
                            "type": "boolean",
                            "description": "Include industry average line. Default true.",
                            "default": True
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "get_measure_performance",
                "description": f"Get measure-level performance data. Can filter by payers, specific measures, domains ({', '.join(f['domains'][:5])}...), or parts (C=medical, D=drug). Use for 'measure scores', 'Part D performance', 'which measures are weak'.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "year": {
                            "type": "integer",
                            "description": f"Year. Default {max(f['star_years'])}.",
                            "default": max(f['star_years'])
                        },
                        "payers": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Parent organizations to filter."
                        },
                        "measure_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Specific measure IDs (e.g., 'C01', 'D08')."
                        },
                        "domains": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": f"Domains: {', '.join(f['domains'])}"
                        },
                        "parts": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Parts: C (medical), D (drug)"
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "get_star_cutpoints",
                "description": "Get star rating cutpoint thresholds by measure and year. Shows what score is needed for each star level (2-5 stars). Use for 'cutpoint thresholds', 'what score for 4 stars', 'how cutpoints changed'.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "years": {
                            "type": "array",
                            "items": {"type": "integer"},
                            "description": "Years to query."
                        },
                        "measure_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Specific measures."
                        },
                        "parts": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Parts: C, D"
                        }
                    },
                    "required": []
                }
            },
            
            # =================================================================
            # RISK SCORE TOOLS
            # =================================================================
            {
                "name": "get_risk_scores_by_payer",
                "description": f"Get risk scores by parent organization. Returns ranking with enrollment, weighted avg risk score. Risk data years: {min(f['risk_years'])}-{max(f['risk_years'])}. Use for 'risk score rankings', 'which payers have highest risk'.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "year": {
                            "type": "integer",
                            "description": f"Year. Range: {min(f['risk_years'])}-{max(f['risk_years'])}. Default {max(f['risk_years'])}.",
                            "default": max(f['risk_years'])
                        },
                        "plan_types": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": f"Plan types: {', '.join(f['plan_types'])}"
                        },
                        "snp_types": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": f"SNP types: {', '.join(f['snp_types'])}"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Number of payers. Default 50.",
                            "default": 50
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "get_risk_scores_timeseries",
                "description": "Get risk score timeseries over years. Use for 'risk score trends', 'how risk changed over time'.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "payers": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Parent organizations to compare."
                        },
                        "metric": {
                            "type": "string",
                            "description": "Metric: 'wavg' (enrollment-weighted, recommended) or 'avg' (simple average).",
                            "default": "wavg"
                        },
                        "include_industry_total": {
                            "type": "boolean",
                            "description": "Include industry average. Default true.",
                            "default": True
                        }
                    },
                    "required": []
                }
            },
            
            # =================================================================
            # CROSS-DOMAIN TOOLS
            # =================================================================
            {
                "name": "get_payer_overview",
                "description": "Get comprehensive overview of a single payer including enrollment, star ratings, and risk scores. Use for 'tell me about Humana', 'UNH overview'.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "payer": {
                            "type": "string",
                            "description": "Payer name. Use common names like 'UNH', 'Humana', 'CVS'."
                        },
                        "year": {
                            "type": "integer",
                            "description": f"Year for snapshot. Default {max(f['years'])}.",
                            "default": max(f['years'])
                        }
                    },
                    "required": ["payer"]
                }
            },
            {
                "name": "compare_payers",
                "description": "Compare multiple payers across enrollment, star ratings, and risk scores over time. Use for 'compare UNH vs Humana', 'top payers comparison'.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "payers": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Payers to compare (2-5 recommended)."
                        },
                        "metrics": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Metrics to compare: 'enrollment', 'fourplus_pct', 'risk_score'. Default all."
                        },
                        "start_year": {
                            "type": "integer",
                            "description": "Start year for comparison.",
                            "default": 2016
                        },
                        "end_year": {
                            "type": "integer",
                            "description": f"End year. Default {max(f['years'])}.",
                            "default": max(f['years'])
                        }
                    },
                    "required": ["payers"]
                }
            },
            
            # =================================================================
            # ANALYTICAL TOOLS
            # =================================================================
            {
                "name": "analyze_star_drops",
                "description": "Find payers who had major drops in 4+ star enrollment percentage and analyze their recovery patterns. Use for 'who had star drops', 'star rating recovery', 'major quality declines'.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "min_drop_pct": {
                            "type": "number",
                            "description": "Minimum drop percentage to flag. Default 30.",
                            "default": 30.0
                        },
                        "min_enrollment": {
                            "type": "integer",
                            "description": "Minimum enrollment to consider. Default 10000.",
                            "default": 10000
                        },
                        "lookback_years": {
                            "type": "integer",
                            "description": "Years to look back. Default 10.",
                            "default": 10
                        }
                    },
                    "required": []
                }
            },
            {
                "name": "get_market_concentration",
                "description": "Calculate market concentration metrics (HHI index, top-N share). Use for 'market concentration', 'HHI', 'how concentrated is MA'.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "year": {
                            "type": "integer",
                            "description": f"Year. Default {max(f['years'])}.",
                            "default": max(f['years'])
                        },
                        "by_state": {
                            "type": "boolean",
                            "description": "Calculate by state instead of national. Default false.",
                            "default": False
                        }
                    },
                    "required": []
                }
            },
            
            # =================================================================
            # METADATA TOOLS
            # =================================================================
            {
                "name": "get_available_filters",
                "description": "Get all available filter options (years, payers, plan types, SNP types, states). Use when user asks about what data is available.",
                "input_schema": {
                    "type": "object",
                    "properties": {},
                    "required": []
                }
            }
        ]
    
    def get_system_prompt_context(self) -> str:
        """
        Get context string for system prompt with key data facts.
        """
        f = self._get_filters()
        
        return f"""
TODAY'S DATE: March 4, 2026

DATA AVAILABILITY:
- Enrollment: Years {min(f['years'])}-{max(f['years'])} (use {max(f['years'])} for "current")
- Star Ratings: Years {min(f['star_years'])}-{max(f['star_years'])}
- Risk Scores: Years {min(f['risk_years'])}-{max(f['risk_years'])}

TOP PAYERS (by enrollment):
{', '.join(f['top_payers'][:15])}
... plus {f['all_payers_count'] - 15} more

PLAN TYPES: {', '.join(f['plan_types'])}
SNP TYPES: {', '.join(f['snp_types'])} (D-SNP = Dual eligible)
STATES: All 50 states plus DC, PR, VI, GU

PAYER NAME ALIASES (system normalizes automatically):
- UNH, United, UnitedHealthcare → UnitedHealth Group, Inc.
- CVS, Aetna → CVS Health Corporation  
- Cigna → The Cigna Group
- Kaiser → Kaiser Foundation Health Plan, Inc.

KEY DATA FACTS:
- Total MA enrollment ~33 million (2026)
- UnitedHealth has ~8-9 million, Humana ~5-6 million
- Risk scores typically range 0.8-1.3 (1.0 = average)
- 4+ star enrollment industrywide ~40-50%

CRITICAL RULES:
1. For "current" or "latest" data, use year 2026
2. Single-year queries should specify the year
3. Timeseries queries get all available years by default
4. Always use tool calls - never generate raw SQL
"""


# Singleton
_definitions_instance = None

def get_tool_definitions() -> ToolDefinitions:
    """Get or create singleton tool definitions."""
    global _definitions_instance
    if _definitions_instance is None:
        _definitions_instance = ToolDefinitions()
    return _definitions_instance
