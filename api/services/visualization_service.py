"""
Visualization Service - Domain-Aware Chart Building
====================================================

Separates "what to visualize" (LLM decision) from "how to visualize" (code).

Architecture:
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│  LLM (Analyzer)                      ChartBuilder                           │
│  ─────────────────                   ────────────────────────────────────   │
│  Outputs VizIntent:                  Takes intent + data:                   │
│  {                                   • Applies domain knowledge             │
│    "viz_type": "trajectory",         • Sets appropriate scales              │
│    "focus": "recovery",              • Chooses colors meaningfully          │
│    "metric": "pct_four_star",        • Validates before output              │
│    "group_by": "parent_org"          • Returns clean chart spec             │
│  }                                                                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

Key Principles:
1. Domain knowledge is in CODE, not prompts (testable, consistent)
2. LLM decides WHAT to show, code decides HOW
3. Every chart is validated before rendering
4. Chart templates encode MA-specific best practices
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Literal, Tuple
from enum import Enum
import re


class VizType(str, Enum):
    """Types of visualizations we support."""
    TRAJECTORY = "trajectory"      # Time series showing evolution (line chart)
    RANKING = "ranking"            # Top/bottom N items (horizontal bar)
    COMPARISON = "comparison"      # Before/after, A vs B (grouped bar)
    DISTRIBUTION = "distribution"  # Spread of values (histogram/box)
    CHANGE = "change"              # Gains vs losses (diverging bar)
    TREND = "trend"                # Simple time series (line)
    TABLE = "table"                # Tabular data (not a chart)


@dataclass
class VizIntent:
    """What the LLM wants to visualize (the WHAT, not the HOW)."""
    viz_type: str
    title: str
    description: str
    
    # Data mapping
    metric: str                           # e.g., "pct_four_star", "enrollment_change"
    dimension: Optional[str] = None       # e.g., "year", "parent_org"
    group_by: Optional[str] = None        # e.g., "parent_org" for multi-line
    
    # Filtering
    filter_field: Optional[str] = None    # e.g., "drop_amount"
    filter_op: Optional[str] = None       # e.g., "lt", "gt", "eq"
    filter_value: Optional[Any] = None    # e.g., -25
    
    # Sorting/limiting
    sort_by: Optional[str] = None
    sort_order: str = "desc"
    limit: Optional[int] = None
    
    # For trajectory charts
    time_field: Optional[str] = None      # e.g., "year", "star_year"
    
    def to_dict(self) -> Dict:
        return {k: v for k, v in self.__dict__.items() if v is not None}


@dataclass 
class ChartSpec:
    """Validated chart specification ready for frontend rendering."""
    chart_type: str                       # "line", "bar", "area"
    title: str
    subtitle: Optional[str] = None
    
    # Data
    data: List[Dict] = field(default_factory=list)
    
    # Encoding
    x_axis: str = ""
    x_label: str = ""
    x_type: str = "category"              # "category", "number", "time"
    
    y_axis: str = ""
    y_label: str = ""
    y_domain: Optional[List[float]] = None  # e.g., [0, 100] for percentages
    
    # Series (for multi-line/grouped charts)
    series: List[Dict] = field(default_factory=list)
    color_field: Optional[str] = None
    
    # Layout
    orientation: str = "vertical"         # "vertical" or "horizontal"
    show_legend: bool = True
    
    # Metadata
    is_valid: bool = True
    validation_errors: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        return {
            "chart_type": self.chart_type,
            "title": self.title,
            "subtitle": self.subtitle,
            "data": self.data,
            "x_axis": self.x_axis,
            "x_label": self.x_label,
            "y_axis": self.y_axis,
            "y_label": self.y_label,
            "y_domain": self.y_domain,
            "series": self.series,
            "color_field": self.color_field,
            "orientation": self.orientation,
            "show_legend": self.show_legend,
        }


@dataclass
class TableSpec:
    """Validated table specification."""
    title: str
    summary: str
    columns: List[str]
    rows: List[Dict]
    
    # Column formatting hints
    column_formats: Dict[str, str] = field(default_factory=dict)  # e.g., {"enrollment": "number", "pct_change": "percent"}
    
    # Highlighting rules
    highlight_rules: List[Dict] = field(default_factory=list)  # e.g., [{"column": "change", "condition": "negative", "style": "red"}]
    
    def to_dict(self) -> Dict:
        return {
            "title": self.title,
            "summary": self.summary,
            "columns": self.columns,
            "rows": self.rows[:100],  # Limit rows
            "column_formats": self.column_formats,
            "highlight_rules": self.highlight_rules,
        }


# =============================================================================
# CHART TEMPLATES - Domain-specific configurations
# =============================================================================

CHART_TEMPLATES = {
    # Star rating trajectories over time
    "star_trajectory": {
        "chart_type": "line",
        "x_type": "category",  # Years as categories, not continuous
        "y_domain": [0, 100],  # Percentages
        "y_label": "% of Enrollment in 4+ Star Plans",
        "colors": ["#3B82F6", "#10B981", "#F59E0B", "#EF4444", "#8B5CF6", "#EC4899"],
    },
    
    # Recovery patterns (special case of trajectory)
    "recovery_trajectory": {
        "chart_type": "line",
        "x_type": "category",
        "y_domain": [0, 100],
        "y_label": "% of Enrollment in 4+ Star Plans",
        "colors": ["#3B82F6", "#10B981", "#F59E0B", "#EF4444"],
        "stroke_dash_field": "recovery_status",  # Dashed lines for non-recoverers
    },
    
    # Top/bottom rankings
    "enrollment_ranking": {
        "chart_type": "bar",
        "orientation": "horizontal",
        "x_label": "Enrollment",
        "colors": ["#3B82F6"],
    },
    
    # Gainers (positive changes)
    "gainers_ranking": {
        "chart_type": "bar",
        "orientation": "horizontal",
        "colors": ["#10B981"],  # Green for gains
        "x_label": "Change",
    },
    
    # Losers (negative changes)
    "losers_ranking": {
        "chart_type": "bar",
        "orientation": "horizontal",
        "colors": ["#EF4444"],  # Red for losses
        "x_label": "Change",
    },
    
    # Before/after comparison
    "before_after": {
        "chart_type": "bar",
        "orientation": "vertical",
        "colors": ["#6B7280", "#3B82F6"],  # Gray (before), Blue (after)
    },
    
    # Drop magnitude
    "drop_magnitude": {
        "chart_type": "bar",
        "orientation": "horizontal",
        "colors": ["#EF4444"],
        "x_label": "Drop (percentage points)",
        "sort": "ascending",  # Most negative at top
    },
}


# =============================================================================
# MA DOMAIN KNOWLEDGE
# =============================================================================

class MADomainKnowledge:
    """Medicare Advantage domain-specific knowledge for visualization."""
    
    # Fields that represent percentages (should have 0-100 scale)
    PERCENTAGE_FIELDS = {
        "pct_four_star", "pct_4_star", "four_star_pct", "before_drop", "after_drop",
        "market_share", "pct_change", "growth_rate", "recovery_rate",
    }
    
    # Fields that represent enrollment counts (should use thousands separator)
    ENROLLMENT_FIELDS = {
        "enrollment", "total_enrollment", "enrollment_at_drop", "member_count",
        "enrollment_change", "dec_enrollment", "feb_enrollment", "dec_2025_enrollment",
        "feb_2026_enrollment",
    }
    
    # Time fields (should be treated as ordinal/categorical, not continuous)
    TIME_FIELDS = {
        "year", "star_year", "drop_year", "month", "period", "years_since_drop",
    }
    
    # Entity/name fields (good for grouping)
    ENTITY_FIELDS = {
        "parent_org", "parent_organization", "contract_id", "plan_name", "payer",
        "organization", "company",
    }
    
    # Status fields (good for coloring)
    STATUS_FIELDS = {
        "recovery_status", "status", "change_type",
    }
    
    # Reasonable ranges for validation
    FIELD_RANGES = {
        "pct_four_star": (0, 100),
        "before_drop": (0, 100),
        "after_drop": (0, 100),
        "drop_amount": (-100, 0),
        "enrollment": (0, 50_000_000),
        "year": (2010, 2030),
    }
    
    @classmethod
    def is_percentage_field(cls, field: str) -> bool:
        return field.lower() in cls.PERCENTAGE_FIELDS or "pct" in field.lower() or "percent" in field.lower()
    
    @classmethod
    def is_enrollment_field(cls, field: str) -> bool:
        return field.lower() in cls.ENROLLMENT_FIELDS or "enrollment" in field.lower()
    
    @classmethod
    def is_time_field(cls, field: str) -> bool:
        return field.lower() in cls.TIME_FIELDS
    
    @classmethod
    def is_entity_field(cls, field: str) -> bool:
        return any(e in field.lower() for e in ["org", "parent", "payer", "company", "plan"])
    
    @classmethod
    def get_field_label(cls, field: str) -> str:
        """Human-readable label for a field."""
        labels = {
            "pct_four_star": "% in 4+ Star Plans",
            "parent_org": "Organization",
            "enrollment": "Enrollment",
            "enrollment_change": "Enrollment Change",
            "drop_amount": "Drop (points)",
            "before_drop": "Before Drop (%)",
            "after_drop": "After Drop (%)",
            "year": "Year",
            "star_year": "Year",
            "drop_year": "Drop Year",
            "years_since_drop": "Years Since Drop",
            "recovery_status": "Recovery Status",
        }
        return labels.get(field, field.replace("_", " ").title())


# =============================================================================
# CHART BUILDER
# =============================================================================

class ChartBuilder:
    """
    Builds validated chart specifications from visualization intents.
    
    Encodes domain knowledge about Medicare Advantage data.
    """
    
    def __init__(self):
        self.domain = MADomainKnowledge()
    
    def build_from_intent(
        self, 
        intent: VizIntent, 
        data: List[Dict],
        data_columns: List[str]
    ) -> ChartSpec:
        """Build a chart from a visualization intent."""
        
        # Route to appropriate builder
        if intent.viz_type == VizType.TRAJECTORY.value:
            return self._build_trajectory(intent, data, data_columns)
        elif intent.viz_type == VizType.RANKING.value:
            return self._build_ranking(intent, data, data_columns)
        elif intent.viz_type == VizType.CHANGE.value:
            return self._build_change_chart(intent, data, data_columns)
        elif intent.viz_type == VizType.COMPARISON.value:
            return self._build_comparison(intent, data, data_columns)
        elif intent.viz_type == VizType.TREND.value:
            return self._build_trend(intent, data, data_columns)
        else:
            # Fallback to auto-detection
            return self._build_auto(intent, data, data_columns)
    
    def _build_trajectory(
        self, 
        intent: VizIntent, 
        data: List[Dict],
        columns: List[str]
    ) -> ChartSpec:
        """Build a trajectory chart (time series by entity)."""
        
        # Determine time field
        time_field = intent.time_field
        if not time_field:
            time_field = next((c for c in columns if self.domain.is_time_field(c)), None)
        
        if not time_field:
            return self._error_spec("No time field found for trajectory chart", intent.title)
        
        # Determine metric field
        metric = intent.metric
        if metric not in columns:
            # Try to find a percentage field
            metric = next((c for c in columns if self.domain.is_percentage_field(c)), columns[0])
        
        # Determine grouping field
        group_by = intent.group_by
        if not group_by:
            group_by = next((c for c in columns if self.domain.is_entity_field(c)), None)
        
        # Get unique entities for series
        if group_by:
            entities = list(set(row.get(group_by) for row in data if row.get(group_by)))
            entities = entities[:10]  # Limit to 10 lines for readability
        else:
            entities = []
        
        # Build pivoted data for multi-line chart
        if group_by and len(entities) > 1:
            # Pivot: each row is a time point, columns are entities
            time_values = sorted(set(row.get(time_field) for row in data if row.get(time_field) is not None))
            pivoted_data = []
            
            for t in time_values:
                row_data = {time_field: t}
                for entity in entities:
                    matching = [r for r in data if r.get(time_field) == t and r.get(group_by) == entity]
                    if matching:
                        row_data[entity] = matching[0].get(metric)
                pivoted_data.append(row_data)
            
            series = [
                {"key": entity, "label": entity[:30], "color": CHART_TEMPLATES["star_trajectory"]["colors"][i % 6]}
                for i, entity in enumerate(entities)
            ]
            
            return ChartSpec(
                chart_type="line",
                title=intent.title,
                subtitle=intent.description,
                data=pivoted_data,
                x_axis=time_field,
                x_label=self.domain.get_field_label(time_field),
                x_type="category",
                y_axis=metric,
                y_label=self.domain.get_field_label(metric),
                y_domain=[0, 100] if self.domain.is_percentage_field(metric) else None,
                series=series,
                show_legend=True,
            )
        else:
            # Single line
            sorted_data = sorted(data, key=lambda x: x.get(time_field, 0))
            return ChartSpec(
                chart_type="line",
                title=intent.title,
                data=sorted_data[:50],
                x_axis=time_field,
                x_label=self.domain.get_field_label(time_field),
                y_axis=metric,
                y_label=self.domain.get_field_label(metric),
                y_domain=[0, 100] if self.domain.is_percentage_field(metric) else None,
                series=[{"key": metric, "label": self.domain.get_field_label(metric), "color": "#3B82F6"}],
            )
    
    def _build_ranking(
        self, 
        intent: VizIntent, 
        data: List[Dict],
        columns: List[str]
    ) -> ChartSpec:
        """Build a ranking chart (horizontal bar)."""
        
        # Determine dimension (what we're ranking)
        dimension = intent.dimension
        if not dimension:
            dimension = next((c for c in columns if self.domain.is_entity_field(c)), columns[0])
        
        # Determine metric
        metric = intent.metric
        if metric not in columns:
            metric = next((c for c in columns if isinstance(data[0].get(c), (int, float))), None)
        
        if not metric:
            return self._error_spec("No numeric metric found for ranking", intent.title)
        
        # Sort and limit data
        sort_order = intent.sort_order or "desc"
        sorted_data = sorted(
            data, 
            key=lambda x: x.get(metric, 0) or 0,
            reverse=(sort_order == "desc")
        )
        limited_data = sorted_data[:intent.limit or 10]
        
        # Determine color based on metric type
        is_negative = any(row.get(metric, 0) < 0 for row in limited_data)
        color = "#EF4444" if is_negative else "#10B981"
        
        return ChartSpec(
            chart_type="bar",
            title=intent.title,
            subtitle=intent.description,
            data=limited_data,
            x_axis=metric,
            x_label=self.domain.get_field_label(metric),
            y_axis=dimension,
            y_label=self.domain.get_field_label(dimension),
            orientation="horizontal",
            series=[{"key": metric, "label": self.domain.get_field_label(metric), "color": color}],
            show_legend=False,
        )
    
    def _build_change_chart(
        self, 
        intent: VizIntent, 
        data: List[Dict],
        columns: List[str]
    ) -> ChartSpec:
        """Build a change chart (diverging bar for gains/losses)."""
        
        # Find the change metric
        metric = intent.metric
        if metric not in columns:
            metric = next((c for c in columns if "change" in c.lower() or "diff" in c.lower()), None)
        
        dimension = intent.dimension
        if not dimension:
            dimension = next((c for c in columns if self.domain.is_entity_field(c)), columns[0])
        
        # Sort by metric value
        sorted_data = sorted(data, key=lambda x: x.get(metric, 0) or 0, reverse=True)
        limited_data = sorted_data[:intent.limit or 15]
        
        return ChartSpec(
            chart_type="bar",
            title=intent.title,
            subtitle=intent.description,
            data=limited_data,
            x_axis=metric,
            x_label=self.domain.get_field_label(metric),
            y_axis=dimension,
            y_label=self.domain.get_field_label(dimension),
            orientation="horizontal",
            series=[{"key": metric, "label": self.domain.get_field_label(metric), "color": "#3B82F6"}],
            color_field=metric,  # Frontend can use this to color positive/negative differently
        )
    
    def _build_comparison(
        self, 
        intent: VizIntent, 
        data: List[Dict],
        columns: List[str]
    ) -> ChartSpec:
        """Build a comparison chart (grouped bar)."""
        
        dimension = intent.dimension or next((c for c in columns if self.domain.is_entity_field(c)), columns[0])
        
        # Find numeric columns for comparison
        numeric_cols = [c for c in columns if isinstance(data[0].get(c), (int, float)) and c != dimension][:2]
        
        if len(numeric_cols) < 2:
            return self._build_ranking(intent, data, columns)
        
        limited_data = data[:15]
        
        return ChartSpec(
            chart_type="bar",
            title=intent.title,
            data=limited_data,
            x_axis=dimension,
            x_label=self.domain.get_field_label(dimension),
            y_axis=numeric_cols[0],
            y_label="Value",
            series=[
                {"key": numeric_cols[0], "label": self.domain.get_field_label(numeric_cols[0]), "color": "#6B7280"},
                {"key": numeric_cols[1], "label": self.domain.get_field_label(numeric_cols[1]), "color": "#3B82F6"},
            ],
        )
    
    def _build_trend(
        self, 
        intent: VizIntent, 
        data: List[Dict],
        columns: List[str]
    ) -> ChartSpec:
        """Build a simple trend chart (single line over time)."""
        
        time_field = intent.time_field or next((c for c in columns if self.domain.is_time_field(c)), None)
        metric = intent.metric
        
        if not time_field:
            return self._build_ranking(intent, data, columns)
        
        sorted_data = sorted(data, key=lambda x: x.get(time_field, 0))
        
        return ChartSpec(
            chart_type="line",
            title=intent.title,
            data=sorted_data[:50],
            x_axis=time_field,
            x_label=self.domain.get_field_label(time_field),
            x_type="category",
            y_axis=metric,
            y_label=self.domain.get_field_label(metric),
            y_domain=[0, 100] if self.domain.is_percentage_field(metric) else None,
            series=[{"key": metric, "label": self.domain.get_field_label(metric), "color": "#3B82F6"}],
        )
    
    def _build_auto(
        self, 
        intent: VizIntent, 
        data: List[Dict],
        columns: List[str]
    ) -> ChartSpec:
        """Auto-detect best chart type based on data."""
        
        if not data:
            return self._error_spec("No data provided", intent.title)
        
        first_row = data[0]
        
        # Check for time series
        time_cols = [c for c in columns if self.domain.is_time_field(c)]
        entity_cols = [c for c in columns if self.domain.is_entity_field(c)]
        numeric_cols = [c for c in columns if isinstance(first_row.get(c), (int, float))]
        
        # Multi-entity time series → trajectory
        if time_cols and entity_cols and len(set(r.get(entity_cols[0]) for r in data)) > 1:
            intent.viz_type = VizType.TRAJECTORY.value
            intent.time_field = time_cols[0]
            intent.group_by = entity_cols[0]
            intent.metric = numeric_cols[0] if numeric_cols else columns[0]
            return self._build_trajectory(intent, data, columns)
        
        # Single time series → trend
        if time_cols and numeric_cols:
            intent.viz_type = VizType.TREND.value
            intent.time_field = time_cols[0]
            intent.metric = numeric_cols[0]
            return self._build_trend(intent, data, columns)
        
        # Entity with numeric → ranking
        if entity_cols and numeric_cols:
            intent.viz_type = VizType.RANKING.value
            intent.dimension = entity_cols[0]
            intent.metric = numeric_cols[0]
            return self._build_ranking(intent, data, columns)
        
        # Fallback: simple bar chart
        return ChartSpec(
            chart_type="bar",
            title=intent.title,
            data=data[:20],
            x_axis=columns[0],
            y_axis=numeric_cols[0] if numeric_cols else columns[1] if len(columns) > 1 else columns[0],
            series=[{"key": numeric_cols[0] if numeric_cols else columns[0], "label": "Value", "color": "#3B82F6"}],
        )
    
    def _error_spec(self, message: str, title: str) -> ChartSpec:
        """Return an error chart spec."""
        return ChartSpec(
            chart_type="bar",
            title=title,
            data=[],
            is_valid=False,
            validation_errors=[message],
        )


# =============================================================================
# CHART VALIDATOR
# =============================================================================

class ChartValidator:
    """Validates chart specifications before rendering."""
    
    def __init__(self):
        self.domain = MADomainKnowledge()
    
    def validate(self, spec: ChartSpec) -> Tuple[bool, List[str]]:
        """Validate a chart spec. Returns (is_valid, list_of_errors)."""
        errors = []
        
        # Check data exists
        if not spec.data:
            errors.append("Chart has no data")
            return False, errors
        
        # Check axes are in data
        first_row = spec.data[0]
        
        if spec.chart_type == "line":
            if spec.x_axis not in first_row:
                errors.append(f"X-axis field '{spec.x_axis}' not in data")
            
            # For multi-series, check series keys
            for s in spec.series:
                if s["key"] not in first_row:
                    # Might be pivoted data, check if key is a value in the data
                    pass
        
        elif spec.chart_type == "bar":
            if spec.orientation == "horizontal":
                if spec.y_axis not in first_row:
                    errors.append(f"Y-axis field '{spec.y_axis}' not in data")
            else:
                if spec.x_axis not in first_row:
                    errors.append(f"X-axis field '{spec.x_axis}' not in data")
        
        # Check for degenerate charts
        if len(spec.data) < 2:
            errors.append("Chart has less than 2 data points")
        
        # Check y_domain makes sense
        if spec.y_domain:
            if spec.y_domain[0] >= spec.y_domain[1]:
                errors.append(f"Invalid y_domain: {spec.y_domain}")
        
        # Check for missing values in key fields
        null_count = sum(1 for row in spec.data if row.get(spec.x_axis) is None)
        if null_count > len(spec.data) * 0.5:
            errors.append(f"More than 50% of x-axis values are null")
        
        return len(errors) == 0, errors
    
    def fix_common_issues(self, spec: ChartSpec) -> ChartSpec:
        """Attempt to fix common chart issues."""
        
        # Fix percentage fields without domain
        if spec.y_axis and self.domain.is_percentage_field(spec.y_axis) and not spec.y_domain:
            spec.y_domain = [0, 100]
        
        # Ensure title isn't too long
        if len(spec.title) > 60:
            spec.title = spec.title[:57] + "..."
        
        # Remove rows with null key values
        if spec.chart_type == "line":
            spec.data = [row for row in spec.data if row.get(spec.x_axis) is not None]
        
        return spec


# =============================================================================
# TABLE BUILDER
# =============================================================================

class TableBuilder:
    """Builds clean table specifications."""
    
    def __init__(self):
        self.domain = MADomainKnowledge()
    
    def build(
        self, 
        title: str, 
        data: List[Dict], 
        columns: Optional[List[str]] = None,
        limit: int = 50
    ) -> TableSpec:
        """Build a table specification."""
        
        if not data:
            return TableSpec(title=title, summary="No data", columns=[], rows=[])
        
        # Determine columns
        if not columns:
            columns = list(data[0].keys())
        
        # Limit columns for readability
        columns = columns[:8]
        
        # Generate column formats
        formats = {}
        first_row = data[0]
        for col in columns:
            val = first_row.get(col)
            if self.domain.is_percentage_field(col):
                formats[col] = "percent"
            elif self.domain.is_enrollment_field(col):
                formats[col] = "number"
            elif isinstance(val, float):
                formats[col] = "decimal"
        
        # Generate highlight rules
        highlights = []
        for col in columns:
            if "change" in col.lower() or "drop" in col.lower():
                highlights.append({
                    "column": col,
                    "condition": "negative",
                    "style": "text-red-600 font-medium",
                })
                highlights.append({
                    "column": col,
                    "condition": "positive", 
                    "style": "text-green-600 font-medium",
                })
        
        return TableSpec(
            title=title,
            summary=f"{min(len(data), limit)} rows",
            columns=columns,
            rows=data[:limit],
            column_formats=formats,
            highlight_rules=highlights,
        )


# =============================================================================
# VISUALIZATION SERVICE (Main Entry Point)
# =============================================================================

class VisualizationService:
    """
    Main service for generating visualizations from data.
    
    Usage:
        viz_service = VisualizationService()
        
        # From intents (preferred - LLM decides what to show)
        charts, tables = viz_service.build_from_intents(intents, data)
        
        # Auto-generate (fallback - code decides)
        charts, tables = viz_service.auto_generate(question, data)
    """
    
    def __init__(self):
        self.chart_builder = ChartBuilder()
        self.table_builder = TableBuilder()
        self.validator = ChartValidator()
    
    def build_from_intents(
        self,
        intents: List[VizIntent],
        data_by_source: Dict[str, Dict],  # {source_id: {"data": [...], "columns": [...]}}
    ) -> Tuple[List[Dict], List[Dict]]:
        """Build visualizations from LLM-generated intents."""
        
        charts = []
        tables = []
        
        for intent in intents:
            # Find data source
            # For now, use first available data source
            for source_id, source_data in data_by_source.items():
                raw_data = source_data.get("data", {})
                
                # Handle SQL result format
                if isinstance(raw_data, dict) and "rows" in raw_data:
                    rows = raw_data.get("rows", [])
                    columns = raw_data.get("columns", [])
                elif isinstance(raw_data, list):
                    rows = raw_data
                    columns = list(raw_data[0].keys()) if raw_data else []
                else:
                    continue
                
                if not rows:
                    continue
                
                if intent.viz_type == VizType.TABLE.value:
                    table_spec = self.table_builder.build(intent.title, rows, columns)
                    tables.append(table_spec.to_dict())
                else:
                    chart_spec = self.chart_builder.build_from_intent(intent, rows, columns)
                    chart_spec = self.validator.fix_common_issues(chart_spec)
                    is_valid, errors = self.validator.validate(chart_spec)
                    
                    if is_valid:
                        charts.append(chart_spec.to_dict())
                    else:
                        # Log validation errors but don't include bad charts
                        print(f"Chart validation failed: {errors}")
                
                break  # Use first matching data source
        
        return charts, tables
    
    def auto_generate(
        self,
        question: str,
        data_by_source: Dict[str, Dict],
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Auto-generate visualizations based on question and data patterns.
        
        This is the fallback when no explicit intents are provided.
        """
        
        charts = []
        tables = []
        
        question_lower = question.lower()
        
        for source_id, source_data in data_by_source.items():
            raw_data = source_data.get("data", {})
            desc = source_data.get("description", "Data")
            
            # Handle SQL result format
            if isinstance(raw_data, dict) and "rows" in raw_data:
                rows = raw_data.get("rows", [])
                columns = raw_data.get("columns", [])
            elif isinstance(raw_data, list):
                rows = raw_data
                columns = list(raw_data[0].keys()) if raw_data else []
            else:
                continue
            
            if not rows:
                continue
            
            # Always create a table
            table_spec = self.table_builder.build(desc[:50], rows, columns)
            tables.append(table_spec.to_dict())
            
            # Infer intent from question
            intent = self._infer_intent_from_question(question, rows, columns, desc)
            
            if intent:
                chart_spec = self.chart_builder.build_from_intent(intent, rows, columns)
                chart_spec = self.validator.fix_common_issues(chart_spec)
                is_valid, errors = self.validator.validate(chart_spec)
                
                if is_valid:
                    charts.append(chart_spec.to_dict())
        
        return charts, tables
    
    def _infer_intent_from_question(
        self,
        question: str,
        data: List[Dict],
        columns: List[str],
        description: str
    ) -> Optional[VizIntent]:
        """Infer visualization intent from question patterns."""
        
        q = question.lower()
        
        # Recovery/trajectory patterns
        if any(word in q for word in ["recover", "trajectory", "over time", "trend", "history"]):
            time_col = next((c for c in columns if MADomainKnowledge.is_time_field(c)), None)
            entity_col = next((c for c in columns if MADomainKnowledge.is_entity_field(c)), None)
            metric_col = next((c for c in columns if MADomainKnowledge.is_percentage_field(c)), None)
            
            if time_col:
                return VizIntent(
                    viz_type=VizType.TRAJECTORY.value,
                    title=f"{description[:40]} Over Time",
                    description="",
                    metric=metric_col or columns[0],
                    time_field=time_col,
                    group_by=entity_col,
                )
        
        # Ranking patterns
        if any(word in q for word in ["top", "largest", "biggest", "most", "best", "ranking"]):
            entity_col = next((c for c in columns if MADomainKnowledge.is_entity_field(c)), columns[0])
            numeric_col = next((c for c in columns if isinstance(data[0].get(c), (int, float))), None)
            
            if numeric_col:
                return VizIntent(
                    viz_type=VizType.RANKING.value,
                    title=f"Top by {MADomainKnowledge.get_field_label(numeric_col)}",
                    description="",
                    metric=numeric_col,
                    dimension=entity_col,
                    limit=10,
                    sort_order="desc",
                )
        
        # Change patterns
        if any(word in q for word in ["gain", "lost", "change", "increase", "decrease", "grew", "drop"]):
            entity_col = next((c for c in columns if MADomainKnowledge.is_entity_field(c)), columns[0])
            change_col = next((c for c in columns if "change" in c.lower() or "drop" in c.lower()), None)
            
            if change_col:
                return VizIntent(
                    viz_type=VizType.CHANGE.value,
                    title=f"Changes: {description[:30]}",
                    description="",
                    metric=change_col,
                    dimension=entity_col,
                    limit=15,
                )
        
        # Comparison patterns
        if any(word in q for word in ["compare", "vs", "versus", "before", "after"]):
            return VizIntent(
                viz_type=VizType.COMPARISON.value,
                title=f"Comparison: {description[:30]}",
                description="",
                metric=columns[0],
            )
        
        # Default: auto-detect
        return VizIntent(
            viz_type="auto",
            title=description[:50],
            description="",
            metric=columns[0] if columns else "",
        )


# =============================================================================
# PARSE VIZ INTENTS FROM LLM OUTPUT
# =============================================================================

def parse_viz_intents(llm_output: str) -> List[VizIntent]:
    """Parse visualization intents from LLM JSON output."""
    
    import json
    import re
    
    intents = []
    
    try:
        # Try to extract JSON
        json_match = re.search(r'```(?:json)?\s*(\{[\s\S]*?\})\s*```', llm_output)
        if json_match:
            json_str = json_match.group(1)
        else:
            json_start = llm_output.find("{")
            if json_start >= 0:
                depth = 0
                end_idx = 0
                for i, c in enumerate(llm_output[json_start:]):
                    if c == '{': depth += 1
                    elif c == '}':
                        depth -= 1
                        if depth == 0:
                            end_idx = json_start + i + 1
                            break
                json_str = llm_output[json_start:end_idx]
            else:
                return intents
        
        parsed = json.loads(json_str)
        
        # Extract viz_intents array
        viz_intents_raw = parsed.get("viz_intents", parsed.get("visualizations", []))
        
        for viz in viz_intents_raw:
            intents.append(VizIntent(
                viz_type=viz.get("viz_type", viz.get("type", "auto")),
                title=viz.get("title", "Chart"),
                description=viz.get("description", ""),
                metric=viz.get("metric", ""),
                dimension=viz.get("dimension"),
                group_by=viz.get("group_by"),
                time_field=viz.get("time_field"),
                filter_field=viz.get("filter_field"),
                filter_value=viz.get("filter_value"),
                sort_by=viz.get("sort_by"),
                sort_order=viz.get("sort_order", "desc"),
                limit=viz.get("limit"),
            ))
    
    except Exception as e:
        print(f"Error parsing viz intents: {e}")
    
    return intents
