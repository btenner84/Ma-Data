"""
Structured Tools for MA Agent V3

Type-safe wrappers around existing services that the LLM calls via function calling.
Each tool has:
- Clear input schema with validation
- Calls existing battle-tested service methods
- Returns structured, predictable output

NO raw SQL - all queries go through existing services.
"""

from typing import Dict, List, Optional, Any, Literal
from dataclasses import dataclass
from datetime import datetime
import difflib

from api.services.enrollment_service import get_enrollment_service
from api.services.stars_service import get_stars_service
from api.services.risk_scores_service import get_risk_scores_service


# =============================================================================
# PAYER NAME NORMALIZATION
# =============================================================================

PAYER_ALIASES = {
    # UnitedHealth
    "unh": "UnitedHealth Group, Inc.",
    "united": "UnitedHealth Group, Inc.",
    "unitedhealthcare": "UnitedHealth Group, Inc.",
    "unitedhealth": "UnitedHealth Group, Inc.",
    
    # Humana
    "humana": "Humana Inc.",
    
    # CVS/Aetna
    "cvs": "CVS Health Corporation",
    "aetna": "CVS Health Corporation",
    "cvs health": "CVS Health Corporation",
    
    # Cigna
    "cigna": "The Cigna Group",
    
    # Centene
    "centene": "Centene Corporation",
    
    # Kaiser
    "kaiser": "Kaiser Foundation Health Plan, Inc.",
    "kaiser permanente": "Kaiser Foundation Health Plan, Inc.",
    
    # Anthem/Elevance
    "anthem": "Elevance Health, Inc.",
    "elevance": "Elevance Health, Inc.",
    
    # Blue Cross/Blue Shield variations
    "bcbs": "BlueCross BlueShield",
    "blue cross": "BlueCross BlueShield",
    
    # Molina
    "molina": "Molina Healthcare, Inc.",
    
    # Wellcare
    "wellcare": "Centene Corporation",  # Acquired by Centene
}


def normalize_payer_name(name: str, valid_payers: List[str]) -> Optional[str]:
    """
    Normalize a payer name input to canonical database value.
    
    1. Check exact match (case-insensitive)
    2. Check alias mapping
    3. Fuzzy match against valid payers
    """
    if not name:
        return None
    
    name_lower = name.lower().strip()
    
    # Check alias mapping first
    if name_lower in PAYER_ALIASES:
        canonical = PAYER_ALIASES[name_lower]
        # Verify it's in valid_payers (handles subsidiaries, mergers)
        if canonical in valid_payers:
            return canonical
        # Try fuzzy match on the canonical name
        matches = difflib.get_close_matches(canonical, valid_payers, n=1, cutoff=0.6)
        if matches:
            return matches[0]
    
    # Check exact match (case-insensitive)
    for payer in valid_payers:
        if payer.lower() == name_lower:
            return payer
    
    # Fuzzy match
    matches = difflib.get_close_matches(name, valid_payers, n=1, cutoff=0.6)
    if matches:
        return matches[0]
    
    return None


def normalize_payer_names(names: List[str], valid_payers: List[str]) -> List[str]:
    """Normalize a list of payer names."""
    normalized = []
    for name in names:
        result = normalize_payer_name(name, valid_payers)
        if result:
            normalized.append(result)
    return list(set(normalized))


# =============================================================================
# TOOL RESULT DATACLASS
# =============================================================================

@dataclass
class ToolResult:
    """Standardized result from any tool."""
    success: bool
    data: Any
    row_count: int
    service_called: str
    audit_id: Optional[str] = None
    error: Optional[str] = None
    validation_warnings: Optional[List[str]] = None


# =============================================================================
# STRUCTURED TOOLS CLASS
# =============================================================================

class StructuredTools:
    """
    Collection of structured tools for the MA Agent.
    Each tool wraps an existing service method with validation.
    """
    
    def __init__(self):
        self.enrollment_service = get_enrollment_service()
        self.stars_service = get_stars_service()
        self.risk_service = get_risk_scores_service()
        
        # Cache filter values for validation
        self._valid_payers: Optional[List[str]] = None
        self._valid_years: Optional[List[int]] = None
        self._valid_states: Optional[List[str]] = None
    
    def _get_valid_payers(self) -> List[str]:
        """Get cached list of valid parent organizations."""
        if self._valid_payers is None:
            filters = self.enrollment_service.get_filters()
            self._valid_payers = filters.get('parent_orgs', [])
        return self._valid_payers
    
    def _get_valid_years(self) -> List[int]:
        """Get cached list of valid years."""
        if self._valid_years is None:
            filters = self.enrollment_service.get_filters()
            self._valid_years = filters.get('years', [])
        return self._valid_years
    
    def _get_valid_states(self) -> List[str]:
        """Get cached list of valid states."""
        if self._valid_states is None:
            filters = self.enrollment_service.get_filters()
            self._valid_states = filters.get('states', [])
        return self._valid_states
    
    # =========================================================================
    # ENROLLMENT TOOLS
    # =========================================================================
    
    def get_enrollment_by_payer(
        self,
        year: int,
        limit: int = 20
    ) -> ToolResult:
        """
        Get enrollment ranking by parent organization for a specific year.
        Returns top payers with enrollment, market share, contract count.
        """
        valid_years = self._get_valid_years()
        if year not in valid_years:
            return ToolResult(
                success=False,
                data=None,
                row_count=0,
                service_called="enrollment_service.get_by_parent_org",
                error=f"Invalid year {year}. Valid range: {min(valid_years)}-{max(valid_years)}"
            )
        
        result = self.enrollment_service.get_by_parent_org(year=year, limit=limit)
        
        return ToolResult(
            success=True,
            data=result.get('data', []),
            row_count=len(result.get('data', [])),
            service_called="enrollment_service.get_by_parent_org",
            audit_id=result.get('audit_id')
        )
    
    def get_enrollment_timeseries(
        self,
        payers: Optional[List[str]] = None,
        plan_types: Optional[List[str]] = None,
        snp_types: Optional[List[str]] = None,
        group_types: Optional[List[str]] = None,
        states: Optional[List[str]] = None,
        start_year: int = 2013,
        end_year: int = 2026
    ) -> ToolResult:
        """
        Get enrollment timeseries with optional filters.
        Can filter by payers, plan types, SNP types, group types, states.
        """
        warnings = []
        
        # Normalize payer names
        normalized_payers = None
        if payers:
            valid_payers = self._get_valid_payers()
            normalized_payers = normalize_payer_names(payers, valid_payers)
            if len(normalized_payers) < len(payers):
                missing = set(payers) - set([p.lower() for p in normalized_payers])
                warnings.append(f"Some payers not found: {missing}")
        
        # For now, get timeseries for first payer (service returns one at a time)
        # TODO: Make service support multiple payers in one call
        all_data = []
        
        if normalized_payers:
            for payer in normalized_payers:
                result = self.enrollment_service.get_timeseries(
                    parent_org=payer,
                    plan_types=plan_types,
                    snp_types=snp_types,
                    group_types=group_types,
                    start_year=start_year,
                    end_year=end_year
                )
                for row in result.get('data', []):
                    row['parent_org'] = payer
                    all_data.append(row)
        else:
            # Industry total
            result = self.enrollment_service.get_timeseries(
                plan_types=plan_types,
                snp_types=snp_types,
                group_types=group_types,
                start_year=start_year,
                end_year=end_year
            )
            for row in result.get('data', []):
                row['parent_org'] = 'Industry Total'
                all_data.append(row)
        
        return ToolResult(
            success=True,
            data=all_data,
            row_count=len(all_data),
            service_called="enrollment_service.get_timeseries",
            audit_id=result.get('audit_id') if 'result' in dir() else None,
            validation_warnings=warnings if warnings else None
        )
    
    def get_enrollment_by_dimension(
        self,
        year: int,
        plan_type: Optional[str] = None,
        snp_type: Optional[str] = None,
        product_type: Optional[str] = None
    ) -> ToolResult:
        """
        Get enrollment breakdown by dimension (plan type, SNP type, etc.).
        """
        result = self.enrollment_service.get_by_dimensions(
            year=year,
            plan_type=plan_type,
            snp_type=snp_type,
            product_type=product_type
        )
        
        return ToolResult(
            success=True,
            data=result.get('data', []),
            row_count=len(result.get('data', [])),
            service_called="enrollment_service.get_by_dimensions",
            audit_id=result.get('audit_id')
        )
    
    def get_enrollment_by_state(
        self,
        year: int
    ) -> ToolResult:
        """
        Get enrollment by state for a specific year.
        """
        result = self.enrollment_service.get_by_state(year=year)
        
        return ToolResult(
            success=True,
            data=result.get('data', []),
            row_count=len(result.get('data', [])),
            service_called="enrollment_service.get_by_state",
            audit_id=result.get('audit_id')
        )
    
    # =========================================================================
    # STARS TOOLS
    # =========================================================================
    
    def get_stars_distribution(
        self,
        payers: Optional[List[str]] = None,
        plan_types: Optional[List[str]] = None,
        snp_types: Optional[List[str]] = None,
        group_types: Optional[List[str]] = None,
        states: Optional[List[str]] = None,
        star_year: Optional[int] = None
    ) -> ToolResult:
        """
        Get star rating distribution (4+ star percentage) with filters.
        Returns timeseries of 4+ star enrollment percentage.
        """
        warnings = []
        
        # Normalize payer names
        normalized_payers = None
        if payers:
            valid_payers = self._get_valid_payers()
            normalized_payers = normalize_payer_names(payers, valid_payers)
            if len(normalized_payers) < len(payers):
                warnings.append(f"Some payers not recognized")
        
        result = self.stars_service.get_distribution(
            parent_orgs=normalized_payers,
            plan_types=plan_types,
            snp_types=snp_types,
            group_types=group_types,
            states=states,
            star_year=star_year
        )
        
        return ToolResult(
            success=True,
            data=result,
            row_count=len(result.get('data', [])),
            service_called="stars_service.get_distribution",
            audit_id=result.get('audit_id'),
            validation_warnings=warnings if warnings else None
        )
    
    def get_stars_by_payer(
        self,
        star_year: int = 2026,
        plan_types: Optional[List[str]] = None,
        snp_types: Optional[List[str]] = None,
        limit: int = 50
    ) -> ToolResult:
        """
        Get star ratings by parent organization for a specific year.
        Returns ranking with enrollment, 4+ star %, weighted average rating.
        """
        result = self.stars_service.get_by_parent(
            star_year=star_year,
            plan_types=plan_types,
            snp_types=snp_types,
            limit=limit
        )
        
        return ToolResult(
            success=True,
            data=result.get('data', []),
            row_count=len(result.get('data', [])),
            service_called="stars_service.get_by_parent",
            audit_id=result.get('audit_id')
        )
    
    def get_stars_timeseries(
        self,
        payers: Optional[List[str]] = None,
        plan_types: Optional[List[str]] = None,
        snp_types: Optional[List[str]] = None,
        group_types: Optional[List[str]] = None,
        include_industry_total: bool = True
    ) -> ToolResult:
        """
        Get star rating timeseries (4+ star % over time).
        """
        warnings = []
        
        normalized_payers = None
        if payers:
            valid_payers = self._get_valid_payers()
            normalized_payers = normalize_payer_names(payers, valid_payers)
        
        result = self.stars_service.get_timeseries(
            parent_orgs=normalized_payers,
            plan_types=plan_types,
            snp_types=snp_types,
            group_types=group_types,
            include_industry_total=include_industry_total
        )
        
        return ToolResult(
            success=True,
            data=result,
            row_count=len(result.get('data', [])),
            service_called="stars_service.get_timeseries",
            audit_id=result.get('audit_id'),
            validation_warnings=warnings if warnings else None
        )
    
    def get_measure_performance(
        self,
        year: int = 2026,
        payers: Optional[List[str]] = None,
        measure_ids: Optional[List[str]] = None,
        domains: Optional[List[str]] = None,
        parts: Optional[List[str]] = None
    ) -> ToolResult:
        """
        Get measure-level performance data.
        Can filter by payers, specific measures, domains, or parts (C/D).
        """
        normalized_payers = None
        if payers:
            valid_payers = self._get_valid_payers()
            normalized_payers = normalize_payer_names(payers, valid_payers)
        
        result = self.stars_service.get_measure_performance(
            year=year,
            parent_orgs=normalized_payers,
            measure_ids=measure_ids,
            domains=domains,
            parts=parts
        )
        
        return ToolResult(
            success=True,
            data=result.get('data', []),
            row_count=len(result.get('data', [])),
            service_called="stars_service.get_measure_performance",
            audit_id=result.get('audit_id')
        )
    
    def get_star_cutpoints(
        self,
        years: Optional[List[int]] = None,
        measure_ids: Optional[List[str]] = None,
        parts: Optional[List[str]] = None
    ) -> ToolResult:
        """
        Get star rating cutpoint thresholds by measure and year.
        Shows what score is needed for each star level.
        """
        result = self.stars_service.get_cutpoints(
            years=years,
            measure_ids=measure_ids,
            parts=parts
        )
        
        return ToolResult(
            success=True,
            data=result.get('data', []),
            row_count=len(result.get('data', [])),
            service_called="stars_service.get_cutpoints",
            audit_id=result.get('audit_id')
        )
    
    # =========================================================================
    # RISK SCORE TOOLS
    # =========================================================================
    
    def get_risk_scores_by_payer(
        self,
        year: int = 2024,
        plan_types: Optional[List[str]] = None,
        snp_types: Optional[List[str]] = None,
        limit: int = 50
    ) -> ToolResult:
        """
        Get risk scores by parent organization for a specific year.
        Returns ranking with enrollment, weighted avg risk score.
        """
        result = self.risk_service.get_by_parent(
            year=year,
            plan_types=plan_types,
            snp_types=snp_types,
            limit=limit
        )
        
        return ToolResult(
            success=True,
            data=result.get('data', []),
            row_count=len(result.get('data', [])),
            service_called="risk_service.get_by_parent",
            audit_id=result.get('audit_id')
        )
    
    def get_risk_scores_timeseries(
        self,
        payers: Optional[List[str]] = None,
        plan_types: Optional[List[str]] = None,
        snp_types: Optional[List[str]] = None,
        group_types: Optional[List[str]] = None,
        metric: str = "wavg",
        include_industry_total: bool = True
    ) -> ToolResult:
        """
        Get risk score timeseries over years.
        
        Metrics:
        - wavg: Enrollment-weighted average (recommended)
        - avg: Simple average
        """
        normalized_payers = None
        if payers:
            valid_payers = self._get_valid_payers()
            normalized_payers = normalize_payer_names(payers, valid_payers)
        
        result = self.risk_service.get_timeseries(
            parent_orgs=normalized_payers,
            plan_types=plan_types,
            snp_types=snp_types,
            group_types=group_types,
            metric=metric,
            include_industry_total=include_industry_total
        )
        
        return ToolResult(
            success=True,
            data=result,
            row_count=len(result.get('years', [])),
            service_called="risk_service.get_timeseries",
            audit_id=result.get('audit_id')
        )
    
    def get_risk_summary(
        self,
        year: Optional[int] = None,
        payers: Optional[List[str]] = None,
        plan_types: Optional[List[str]] = None,
        snp_types: Optional[List[str]] = None
    ) -> ToolResult:
        """
        Get risk score summary statistics.
        Returns contract count, enrollment, weighted avg, min/max.
        """
        normalized_payers = None
        if payers:
            valid_payers = self._get_valid_payers()
            normalized_payers = normalize_payer_names(payers, valid_payers)
        
        result = self.risk_service.get_summary(
            year=year,
            parent_orgs=normalized_payers,
            plan_types=plan_types,
            snp_types=snp_types
        )
        
        return ToolResult(
            success=True,
            data=result.get('data', []),
            row_count=len(result.get('data', [])),
            service_called="risk_service.get_summary",
            audit_id=result.get('audit_id')
        )
    
    # =========================================================================
    # CROSS-DOMAIN TOOLS
    # =========================================================================
    
    def get_payer_overview(
        self,
        payer: str,
        year: int = 2026
    ) -> ToolResult:
        """
        Get comprehensive overview of a single payer.
        Includes enrollment, star ratings, and risk scores.
        """
        valid_payers = self._get_valid_payers()
        normalized_payer = normalize_payer_name(payer, valid_payers)
        
        if not normalized_payer:
            return ToolResult(
                success=False,
                data=None,
                row_count=0,
                service_called="multiple",
                error=f"Payer '{payer}' not found. Try one of: {', '.join(valid_payers[:10])}..."
            )
        
        # Get enrollment
        enrollment_result = self.enrollment_service.get_timeseries(
            parent_org=normalized_payer,
            start_year=year,
            end_year=year
        )
        
        # Get stars
        stars_result = self.stars_service.get_by_parent(
            star_year=year
        )
        payer_stars = next(
            (r for r in stars_result.get('data', []) if r.get('parent_org') == normalized_payer),
            None
        )
        
        # Get risk scores (use 2024 as latest available)
        risk_year = min(year, 2024)
        risk_result = self.risk_service.get_by_parent(year=risk_year)
        payer_risk = next(
            (r for r in risk_result.get('data', []) if r.get('parent_org') == normalized_payer),
            None
        )
        
        overview = {
            'parent_org': normalized_payer,
            'year': year,
            'enrollment': enrollment_result.get('data', [{}])[0] if enrollment_result.get('data') else {},
            'stars': payer_stars,
            'risk_scores': payer_risk
        }
        
        return ToolResult(
            success=True,
            data=overview,
            row_count=1,
            service_called="multiple (enrollment, stars, risk)"
        )
    
    def compare_payers(
        self,
        payers: List[str],
        metrics: Optional[List[str]] = None,
        start_year: int = 2016,
        end_year: int = 2026
    ) -> ToolResult:
        """
        Compare multiple payers across enrollment, stars, and risk scores.
        
        Metrics options: enrollment, fourplus_pct, risk_score (or all by default)
        """
        valid_payers = self._get_valid_payers()
        normalized_payers = normalize_payer_names(payers, valid_payers)
        
        if not normalized_payers:
            return ToolResult(
                success=False,
                data=None,
                row_count=0,
                service_called="multiple",
                error=f"No valid payers found in: {payers}"
            )
        
        # Default metrics
        if not metrics:
            metrics = ['enrollment', 'fourplus_pct', 'risk_score']
        
        comparison = {
            'payers': normalized_payers,
            'years': list(range(start_year, end_year + 1)),
            'metrics': {}
        }
        
        # Get enrollment timeseries
        if 'enrollment' in metrics:
            enrollment_data = {}
            for payer in normalized_payers:
                result = self.enrollment_service.get_timeseries(
                    parent_org=payer,
                    start_year=start_year,
                    end_year=end_year
                )
                enrollment_data[payer] = {
                    row['year']: row.get('enrollment', 0)
                    for row in result.get('data', [])
                }
            comparison['metrics']['enrollment'] = enrollment_data
        
        # Get stars timeseries
        if 'fourplus_pct' in metrics:
            stars_result = self.stars_service.get_distribution(
                parent_orgs=normalized_payers
            )
            comparison['metrics']['fourplus_pct'] = stars_result.get('series', {})
        
        # Get risk scores timeseries
        if 'risk_score' in metrics:
            risk_result = self.risk_service.get_timeseries(
                parent_orgs=normalized_payers,
                include_industry_total=False
            )
            comparison['metrics']['risk_score'] = risk_result.get('series', {})
        
        return ToolResult(
            success=True,
            data=comparison,
            row_count=len(normalized_payers),
            service_called="multiple (enrollment, stars, risk)"
        )
    
    # =========================================================================
    # ANALYTICAL TOOLS
    # =========================================================================
    
    def analyze_star_drops(
        self,
        min_drop_pct: float = 30.0,
        min_enrollment: int = 10000,
        lookback_years: int = 10
    ) -> ToolResult:
        """
        Find payers who had major drops in 4+ star enrollment percentage
        and analyze their recovery patterns.
        
        Returns list of drop events with:
        - payer, year of drop, drop magnitude
        - subsequent years and whether recovered
        """
        # Get full stars timeseries for all payers
        stars_result = self.stars_service.get_distribution()
        
        years = stars_result.get('years', [])
        series = stars_result.get('series', {})
        
        drop_events = []
        
        for payer, pcts in series.items():
            if payer == 'Industry Total':
                continue
            
            for i in range(1, len(pcts)):
                prev_pct = pcts[i-1]
                curr_pct = pcts[i]
                
                if prev_pct is None or curr_pct is None:
                    continue
                
                drop = prev_pct - curr_pct
                
                if drop >= min_drop_pct:
                    # Found a major drop
                    year = years[i]
                    
                    # Check recovery in subsequent years
                    recovery_years = []
                    for j in range(i+1, len(pcts)):
                        if pcts[j] is not None:
                            recovery_years.append({
                                'year': years[j],
                                'fourplus_pct': pcts[j],
                                'recovered': pcts[j] >= prev_pct * 0.8  # 80% of original
                            })
                    
                    drop_events.append({
                        'parent_org': payer,
                        'drop_year': year,
                        'pre_drop_pct': prev_pct,
                        'post_drop_pct': curr_pct,
                        'drop_magnitude': round(drop, 1),
                        'recovery': recovery_years
                    })
        
        # Sort by drop magnitude
        drop_events.sort(key=lambda x: x['drop_magnitude'], reverse=True)
        
        return ToolResult(
            success=True,
            data=drop_events,
            row_count=len(drop_events),
            service_called="stars_service.get_distribution (analytical)"
        )
    
    def get_market_concentration(
        self,
        year: int = 2026,
        by_state: bool = False
    ) -> ToolResult:
        """
        Calculate market concentration metrics (HHI, top-N share).
        """
        if by_state:
            result = self.enrollment_service.get_by_state(year=year)
            data = result.get('data', [])
        else:
            result = self.enrollment_service.get_by_parent_org(year=year, limit=100)
            data = result.get('data', [])
        
        # Calculate HHI
        total = sum(r.get('total_enrollment', 0) for r in data)
        hhi = sum(
            ((r.get('total_enrollment', 0) / total * 100) ** 2)
            for r in data
        ) if total > 0 else 0
        
        # Top N concentration
        sorted_data = sorted(data, key=lambda x: x.get('total_enrollment', 0), reverse=True)
        top_3 = sum(r.get('total_enrollment', 0) for r in sorted_data[:3]) / total * 100 if total > 0 else 0
        top_5 = sum(r.get('total_enrollment', 0) for r in sorted_data[:5]) / total * 100 if total > 0 else 0
        top_10 = sum(r.get('total_enrollment', 0) for r in sorted_data[:10]) / total * 100 if total > 0 else 0
        
        return ToolResult(
            success=True,
            data={
                'year': year,
                'level': 'state' if by_state else 'national',
                'hhi': round(hhi, 2),
                'hhi_interpretation': 'highly concentrated' if hhi > 2500 else 'moderately concentrated' if hhi > 1500 else 'competitive',
                'top_3_share': round(top_3, 1),
                'top_5_share': round(top_5, 1),
                'top_10_share': round(top_10, 1),
                'total_enrollment': total,
                'entity_count': len(data)
            },
            row_count=1,
            service_called="enrollment_service.get_by_parent_org (analytical)",
            audit_id=result.get('audit_id')
        )
    
    # =========================================================================
    # FILTER/METADATA TOOLS
    # =========================================================================
    
    def get_available_filters(self) -> ToolResult:
        """
        Get all available filter options for the data.
        Returns valid years, payers, plan types, SNP types, states.
        """
        enrollment_filters = self.enrollment_service.get_filters()
        stars_filters = self.stars_service.get_filters()
        risk_filters = self.risk_service.get_filters()
        
        return ToolResult(
            success=True,
            data={
                'years': {
                    'enrollment': enrollment_filters.get('years', []),
                    'stars': stars_filters.get('star_years', []),
                    'risk': risk_filters.get('years', [])
                },
                'parent_orgs': enrollment_filters.get('parent_orgs', []),
                'plan_types': enrollment_filters.get('plan_types', []),
                'plan_types_simplified': enrollment_filters.get('plan_types_simplified', []),
                'snp_types': enrollment_filters.get('snp_types', []),
                'states': enrollment_filters.get('states', []),
                'measure_domains': stars_filters.get('domains', []),
                'measure_parts': stars_filters.get('parts', [])
            },
            row_count=1,
            service_called="multiple filter services"
        )


# Singleton instance
_tools_instance = None

def get_structured_tools() -> StructuredTools:
    """Get or create singleton structured tools instance."""
    global _tools_instance
    if _tools_instance is None:
        _tools_instance = StructuredTools()
    return _tools_instance
