"""
MA Knowledge Extraction Pipeline
=================================

Extracts structured data from CMS documents (rate notices, tech notes, etc.)
into queryable format. This is the foundation for true MA intelligence.

Document Types & Key Fields:
1. Rate Notices (Advance & Final)
   - MA benchmark/growth rates
   - Risk adjustment parameters
   - Star ratings bonus structure
   - Part D parameters
   
2. Star Ratings Technical Notes
   - Measure definitions
   - Cutpoint methodology
   - Weight assignments
   - Domain structure

3. Risk Adjustment Documentation
   - HCC model versions and coefficients
   - Normalization factors
   - Phase-in schedules
"""

import os
import re
import json
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict, field
from datetime import datetime
import boto3


# =============================================================================
# STRUCTURED SCHEMAS
# =============================================================================

@dataclass
class RateNoticeMetrics:
    """Structured metrics from MA Rate Notice (Advance or Final)."""
    year: int
    notice_type: str  # "advance" or "final"
    publication_date: Optional[str] = None
    
    # MA Capitation / Benchmark
    ma_growth_rate: Optional[float] = None  # % change in per capita MA growth
    effective_growth_rate: Optional[float] = None  # After all adjustments
    fee_for_service_growth: Optional[float] = None  # FFS comparison
    coding_intensity_adjustment: Optional[float] = None  # Negative adjustment %
    
    # Risk Adjustment
    risk_model_version: Optional[str] = None  # e.g., "V28"
    prior_model_version: Optional[str] = None  # e.g., "V24"
    model_phasein_current_pct: Optional[float] = None  # % of new model
    model_phasein_prior_pct: Optional[float] = None  # % of prior model
    normalization_factor: Optional[float] = None
    frailty_adjustment: Optional[float] = None
    
    # ESRD
    esrd_model_update: Optional[str] = None
    esrd_dialysis_rate_change: Optional[float] = None
    
    # Star Ratings / Quality
    star_bonus_5star: Optional[float] = None  # 5%
    star_bonus_4plus: Optional[float] = None  # typically 5%
    star_bonus_35: Optional[float] = None  # typically 0% or 3%
    rebate_pct_5star: Optional[float] = None  # 70%
    rebate_pct_4plus: Optional[float] = None  # 65%
    rebate_pct_35: Optional[float] = None  # 65%
    rebate_pct_below35: Optional[float] = None  # 50%
    
    # Part D
    part_d_base_premium: Optional[float] = None
    part_d_deductible: Optional[float] = None
    part_d_initial_coverage_limit: Optional[float] = None
    part_d_oop_threshold: Optional[float] = None  # Catastrophic threshold
    part_d_low_income_subsidy_update: Optional[str] = None
    part_d_dir_changes: Optional[str] = None  # Direct/Indirect Remuneration
    
    # IRA-Related (Inflation Reduction Act)
    ira_insulin_cap: Optional[float] = None  # $35
    ira_oop_cap: Optional[float] = None  # $2000 cap
    ira_inflation_rebate_update: Optional[str] = None
    
    # Key Policy Changes (list of significant changes)
    key_policy_changes: List[str] = field(default_factory=list)
    
    # Network & Access
    network_adequacy_changes: Optional[str] = None
    prior_auth_changes: Optional[str] = None
    telehealth_policy: Optional[str] = None
    
    # SNP Requirements
    snp_model_of_care_changes: Optional[str] = None
    dsnp_integration_requirements: Optional[str] = None
    
    # Metadata
    source_url: Optional[str] = None
    extracted_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class StarMeasureDefinition:
    """Structured definition of a Star Ratings measure."""
    measure_id: str  # e.g., "C01"
    measure_key: str  # normalized key
    measure_name: str
    part: str  # "C" or "D"
    domain: str  # e.g., "Staying Healthy", "Managing Chronic Conditions"
    
    # Definition
    description: str = ""
    data_source: str = ""  # e.g., "HEDIS", "CAHPS", "HOS", "CMS Admin"
    numerator_definition: str = ""
    denominator_definition: str = ""
    
    # Scoring
    weight: float = 1.0
    higher_is_better: bool = True
    reduction_applicable: bool = False
    improvement_applicable: bool = False
    
    # Cutpoints (can vary by year)
    cutpoints_by_year: Dict[int, Dict[str, float]] = field(default_factory=dict)
    
    # History
    first_year: Optional[int] = None
    last_year: Optional[int] = None  # None if still active
    name_changes: List[Dict] = field(default_factory=list)  # {year, old_name, new_name}
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass 
class HCCModelParameters:
    """Structured parameters for a CMS-HCC risk adjustment model version."""
    model_version: str  # e.g., "V28"
    model_year: int  # Year the model is used for payment
    
    # Phase-in schedule
    phasein_pct: float  # % this model is used
    blended_with: Optional[str] = None  # Prior model version if blended
    blended_pct: Optional[float] = None  # % of prior model
    
    # Key parameters
    normalization_factor: Optional[float] = None
    coding_intensity_factor: Optional[float] = None
    
    # Model structure
    total_hccs: Optional[int] = None
    hcc_groups: List[str] = field(default_factory=list)  # Major HCC categories
    
    # Segments
    segments: List[str] = field(default_factory=list)  # e.g., ["Community Aged", "Community Disabled", "Institutional", "ESRD"]
    
    # Key changes from prior version
    changes_from_prior: List[str] = field(default_factory=list)
    
    # Coefficient highlights (top impactful HCCs)
    top_hccs_by_coefficient: List[Dict] = field(default_factory=list)  # [{hcc, coefficient, description}]
    
    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class PolicyChange:
    """A specific policy change with effective date and details."""
    change_id: str
    effective_year: int
    category: str  # "risk_adjustment", "star_ratings", "part_d", "network", "snp", etc.
    title: str
    description: str
    
    # Impact assessment
    impact_level: str = "medium"  # "high", "medium", "low"
    affected_entities: List[str] = field(default_factory=list)  # ["MA plans", "Part D sponsors", "SNPs"]
    
    # Source
    source_document: str = ""  # e.g., "2027 Advance Notice"
    source_page: Optional[int] = None
    
    # Timeline
    announced_date: Optional[str] = None
    finalized_date: Optional[str] = None
    implementation_date: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return asdict(self)


# =============================================================================
# EXTRACTION PATTERNS
# =============================================================================

class RateNoticeExtractor:
    """
    Extracts structured data from rate notice PDF text.
    Uses regex patterns and heuristics to find key metrics.
    """
    
    # Patterns for extracting specific values
    PATTERNS = {
        # Growth rates
        'ma_growth_rate': [
            r'(?:MA\s+)?growth\s+(?:rate|percentage)[^\d]*(\d+\.?\d*)\s*(?:percent|%)',
            r'per\s+capita\s+growth[^\d]*(\d+\.?\d*)\s*(?:percent|%)',
            r'capitation\s+rate.*?increase[^\d]*(\d+\.?\d*)\s*(?:percent|%)',
            r'(\d+\.?\d*)\s*(?:percent|%)\s+(?:growth|increase)',
        ],
        'effective_growth_rate': [
            r'effective\s+growth[^\d]*(\d+\.?\d*)\s*(?:percent|%)',
            r'after\s+adjustments[^\d]*(\d+\.?\d*)\s*(?:percent|%)',
            r'net\s+(?:growth|change)[^\d]*(\d+\.?\d*)\s*(?:percent|%)',
        ],
        'coding_intensity_adjustment': [
            r'coding\s+(?:intensity|pattern)\s+adjustment[^\d]*(-?\d+\.?\d*)\s*(?:percent|%)',
            r'coding\s+adjustment[^\d]*(-?\d+\.?\d*)\s*(?:percent|%)',
        ],
        
        # Risk adjustment
        'normalization_factor': [
            r'normalization\s+factor[^\d]*(\d+\.?\d*)',
            r'risk\s+score\s+normalization[^\d]*(\d+\.?\d*)',
        ],
        'model_phasein': [
            r'(\d+)\s*(?:percent|%)\s+(?:V28|new\s+model)',
            r'V28[^\d]*(\d+)\s*(?:percent|%)',
            r'phase[- ]?in[^\d]*(\d+)\s*(?:percent|%)',
        ],
        
        # Star ratings
        'star_bonus': [
            r'(?:quality\s+)?bonus[^\d]*(\d+\.?\d*)\s*(?:percent|%)\s+(?:for\s+)?(?:4|four|4\.0|4\+)',
            r'5[- ]?star[^\d]*bonus[^\d]*(\d+\.?\d*)\s*(?:percent|%)',
        ],
        'rebate_pct': [
            r'rebate[^\d]*(\d+\.?\d*)\s*(?:percent|%)',
            r'(\d+\.?\d*)\s*(?:percent|%)\s+rebate',
        ],
        
        # Part D
        'part_d_deductible': [
            r'(?:Part\s+D\s+)?deductible[^\d]*\$?\s*(\d+(?:,\d+)?(?:\.\d+)?)',
            r'\$?\s*(\d+(?:,\d+)?(?:\.\d+)?)\s+deductible',
        ],
        'part_d_oop_threshold': [
            r'(?:out[- ]?of[- ]?pocket|OOP)\s+(?:threshold|limit|cap)[^\d]*\$?\s*(\d+(?:,\d+)?(?:\.\d+)?)',
            r'catastrophic\s+(?:threshold|coverage)[^\d]*\$?\s*(\d+(?:,\d+)?(?:\.\d+)?)',
        ],
        'ira_oop_cap': [
            r'\$2[,]?000\s+(?:cap|limit|out[- ]?of[- ]?pocket)',
            r'(?:IRA|Inflation\s+Reduction\s+Act)[^\$]*\$?\s*(\d+(?:,\d+)?)\s+(?:cap|limit)',
        ],
    }
    
    def extract(self, text: str, year: int, notice_type: str) -> RateNoticeMetrics:
        """Extract structured metrics from rate notice text."""
        metrics = RateNoticeMetrics(year=year, notice_type=notice_type)
        
        text_lower = text.lower()
        
        # Extract each metric using patterns
        for field_name, patterns in self.PATTERNS.items():
            for pattern in patterns:
                match = re.search(pattern, text_lower)
                if match:
                    try:
                        value = match.group(1)
                        # Clean and convert
                        value = value.replace(',', '')
                        if hasattr(metrics, field_name):
                            field_type = type(getattr(metrics, field_name))
                            if field_type == float or getattr(metrics, field_name) is None:
                                setattr(metrics, field_name, float(value))
                    except (ValueError, IndexError):
                        pass
                    break
        
        # Extract key policy changes (look for bullet points or numbered items)
        policy_changes = self._extract_policy_changes(text)
        metrics.key_policy_changes = policy_changes[:20]  # Top 20
        
        # Extract publication date
        date_patterns = [
            r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}',
            r'\d{1,2}/\d{1,2}/\d{4}',
        ]
        for pattern in date_patterns:
            match = re.search(pattern, text)
            if match:
                metrics.publication_date = match.group(0)
                break
        
        # Set known values based on year (these are often standard)
        self._set_standard_values(metrics, year)
        
        return metrics
    
    def _extract_policy_changes(self, text: str) -> List[str]:
        """Extract key policy changes from document."""
        changes = []
        
        # Look for sections with changes
        change_markers = [
            'key change', 'significant change', 'policy change', 'update',
            'modification', 'revision', 'new for', 'beginning in'
        ]
        
        lines = text.split('\n')
        in_change_section = False
        
        for i, line in enumerate(lines):
            line_lower = line.lower().strip()
            
            # Check if entering a change section
            if any(marker in line_lower for marker in change_markers):
                in_change_section = True
            
            # Look for bullet points or numbered items
            if line.strip().startswith(('•', '-', '●', '○', '*')) or re.match(r'^\d+[.)]', line.strip()):
                content = re.sub(r'^[•\-●○*\d.)\s]+', '', line).strip()
                if len(content) > 30 and len(content) < 500:
                    changes.append(content)
        
        return changes
    
    def _set_standard_values(self, metrics: RateNoticeMetrics, year: int):
        """Set standard/known values for the year."""
        # Star bonus structure (these are typically consistent)
        if metrics.star_bonus_5star is None:
            metrics.star_bonus_5star = 5.0
        if metrics.star_bonus_4plus is None:
            metrics.star_bonus_4plus = 5.0
        if metrics.star_bonus_35 is None:
            metrics.star_bonus_35 = 0.0
        
        # Rebate percentages (statutory)
        if metrics.rebate_pct_5star is None:
            metrics.rebate_pct_5star = 70.0
        if metrics.rebate_pct_4plus is None:
            metrics.rebate_pct_4plus = 65.0
        if metrics.rebate_pct_35 is None:
            metrics.rebate_pct_35 = 65.0
        if metrics.rebate_pct_below35 is None:
            metrics.rebate_pct_below35 = 50.0
        
        # V28 phase-in schedule (known)
        v28_phasein = {
            2024: (33, 67, "V28", "V24"),  # 33% V28, 67% V24
            2025: (67, 33, "V28", "V24"),  # 67% V28, 33% V24
            2026: (100, 0, "V28", None),   # 100% V28
            2027: (100, 0, "V28", None),   # 100% V28
        }
        
        if year in v28_phasein:
            pct, prior_pct, model, prior_model = v28_phasein[year]
            metrics.model_phasein_current_pct = pct
            metrics.model_phasein_prior_pct = prior_pct
            metrics.risk_model_version = model
            metrics.prior_model_version = prior_model
        
        # IRA caps (known values)
        if year >= 2024:
            metrics.ira_insulin_cap = 35.0
        if year >= 2025:
            metrics.ira_oop_cap = 2000.0


# =============================================================================
# KNOWLEDGE STORE
# =============================================================================

class MAKnowledgeStore:
    """
    Stores and retrieves structured MA knowledge.
    Backed by S3 JSON files for persistence.
    """
    
    def __init__(self, bucket: str = None, prefix: str = "knowledge"):
        self.bucket = bucket or os.environ.get("S3_BUCKET", "ma-data123")
        self.prefix = prefix
        self.s3 = boto3.client('s3')
        
        # In-memory cache
        self._rate_notices: Dict[Tuple[int, str], RateNoticeMetrics] = {}
        self._measures: Dict[str, StarMeasureDefinition] = {}
        self._hcc_models: Dict[str, HCCModelParameters] = {}
        self._policy_changes: List[PolicyChange] = []
        
        # Load existing data
        self._load_all()
    
    def _load_all(self):
        """Load all knowledge from S3."""
        self._load_rate_notices()
        self._load_hcc_models()
        self._load_policy_changes()
    
    def _load_rate_notices(self):
        """Load rate notice metrics from S3."""
        try:
            response = self.s3.get_object(
                Bucket=self.bucket,
                Key=f"{self.prefix}/rate_notices.json"
            )
            data = json.loads(response['Body'].read().decode('utf-8'))
            for item in data:
                key = (item['year'], item['notice_type'])
                self._rate_notices[key] = RateNoticeMetrics(**item)
            print(f"Loaded {len(self._rate_notices)} rate notice records")
        except Exception as e:
            print(f"No existing rate notices found: {e}")
    
    def _load_hcc_models(self):
        """Load HCC model parameters from S3."""
        try:
            response = self.s3.get_object(
                Bucket=self.bucket,
                Key=f"{self.prefix}/hcc_models.json"
            )
            data = json.loads(response['Body'].read().decode('utf-8'))
            for item in data:
                key = f"{item['model_version']}_{item['model_year']}"
                self._hcc_models[key] = HCCModelParameters(**item)
            print(f"Loaded {len(self._hcc_models)} HCC model records")
        except Exception as e:
            print(f"No existing HCC models found: {e}")
    
    def _load_policy_changes(self):
        """Load policy changes from S3."""
        try:
            response = self.s3.get_object(
                Bucket=self.bucket,
                Key=f"{self.prefix}/policy_changes.json"
            )
            data = json.loads(response['Body'].read().decode('utf-8'))
            self._policy_changes = [PolicyChange(**item) for item in data]
            print(f"Loaded {len(self._policy_changes)} policy changes")
        except Exception as e:
            print(f"No existing policy changes found: {e}")
    
    def save_rate_notice(self, metrics: RateNoticeMetrics):
        """Save rate notice metrics."""
        key = (metrics.year, metrics.notice_type)
        self._rate_notices[key] = metrics
        self._persist_rate_notices()
    
    def _persist_rate_notices(self):
        """Persist rate notices to S3."""
        data = [m.to_dict() for m in self._rate_notices.values()]
        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=f"{self.prefix}/rate_notices.json",
                Body=json.dumps(data, indent=2, default=str),
                ContentType='application/json'
            )
        except Exception as e:
            print(f"Error saving rate notices: {e}")
    
    def get_rate_notice(self, year: int, notice_type: str = "advance") -> Optional[RateNoticeMetrics]:
        """Get rate notice metrics for a year."""
        return self._rate_notices.get((year, notice_type))
    
    def get_all_rate_notices(self) -> List[RateNoticeMetrics]:
        """Get all rate notice metrics."""
        return list(self._rate_notices.values())
    
    def save_hcc_model(self, params: HCCModelParameters):
        """Save HCC model parameters."""
        key = f"{params.model_version}_{params.model_year}"
        self._hcc_models[key] = params
        self._persist_hcc_models()
    
    def _persist_hcc_models(self):
        """Persist HCC models to S3."""
        data = [m.to_dict() for m in self._hcc_models.values()]
        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=f"{self.prefix}/hcc_models.json",
                Body=json.dumps(data, indent=2, default=str),
                ContentType='application/json'
            )
        except Exception as e:
            print(f"Error saving HCC models: {e}")
    
    def get_hcc_model(self, model_version: str, year: int) -> Optional[HCCModelParameters]:
        """Get HCC model parameters."""
        return self._hcc_models.get(f"{model_version}_{year}")
    
    def add_policy_change(self, change: PolicyChange):
        """Add a policy change."""
        self._policy_changes.append(change)
        self._persist_policy_changes()
    
    def _persist_policy_changes(self):
        """Persist policy changes to S3."""
        data = [c.to_dict() for c in self._policy_changes]
        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=f"{self.prefix}/policy_changes.json",
                Body=json.dumps(data, indent=2, default=str),
                ContentType='application/json'
            )
        except Exception as e:
            print(f"Error saving policy changes: {e}")
    
    def get_policy_changes(
        self, 
        year: Optional[int] = None, 
        category: Optional[str] = None
    ) -> List[PolicyChange]:
        """Get policy changes with optional filters."""
        changes = self._policy_changes
        if year:
            changes = [c for c in changes if c.effective_year == year]
        if category:
            changes = [c for c in changes if c.category == category]
        return sorted(changes, key=lambda x: x.effective_year, reverse=True)
    
    def query(self, question: str) -> Dict[str, Any]:
        """
        Natural language query interface for the knowledge store.
        Returns structured data relevant to the question.
        """
        question_lower = question.lower()
        result = {
            "question": question,
            "data": {},
            "sources": []
        }
        
        # Check for year references
        year_match = re.search(r'20\d{2}', question)
        year = int(year_match.group(0)) if year_match else None
        
        # Rate notice queries
        if any(term in question_lower for term in ['rate notice', 'advance notice', 'announcement', 'growth rate', 'benchmark']):
            if year:
                notice = self.get_rate_notice(year, 'advance')
                if not notice:
                    notice = self.get_rate_notice(year, 'final')
                if notice:
                    result['data']['rate_notice'] = notice.to_dict()
                    result['sources'].append(f"{year} {'Advance' if notice.notice_type == 'advance' else 'Final'} Notice")
        
        # Risk adjustment queries
        if any(term in question_lower for term in ['risk adjustment', 'hcc', 'v28', 'v24', 'normalization']):
            if year:
                for version in ['V28', 'V24', 'V22']:
                    model = self.get_hcc_model(version, year)
                    if model:
                        result['data']['hcc_model'] = model.to_dict()
                        result['sources'].append(f"CMS-HCC {version} Model ({year})")
                        break
        
        # Policy change queries
        if any(term in question_lower for term in ['policy', 'change', 'update', 'new']):
            changes = self.get_policy_changes(year=year)[:10]
            if changes:
                result['data']['policy_changes'] = [c.to_dict() for c in changes]
                result['sources'].append("CMS Policy Changes Database")
        
        return result


# =============================================================================
# EXTRACTION PIPELINE
# =============================================================================

class KnowledgeExtractionPipeline:
    """
    Main pipeline that processes CMS documents and extracts structured knowledge.
    """
    
    def __init__(self, bucket: str = None):
        self.bucket = bucket or os.environ.get("S3_BUCKET", "ma-data123")
        self.s3 = boto3.client('s3')
        self.knowledge_store = MAKnowledgeStore(bucket=self.bucket)
        self.rate_notice_extractor = RateNoticeExtractor()
    
    def process_rate_notice(self, year: int, notice_type: str = "advance") -> RateNoticeMetrics:
        """
        Process a rate notice and extract structured metrics.
        """
        # Load document text from S3
        text_key = f"documents/text/rate_notice_{notice_type}/{year}.txt"
        
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=text_key)
            text = response['Body'].read().decode('utf-8')
        except Exception as e:
            print(f"Error loading document text: {e}")
            return None
        
        # Extract metrics
        metrics = self.rate_notice_extractor.extract(text, year, notice_type)
        
        # Get URL from metadata
        try:
            meta_key = f"documents/metadata/rate_notice_{notice_type}/{year}.json"
            meta_response = self.s3.get_object(Bucket=self.bucket, Key=meta_key)
            metadata = json.loads(meta_response['Body'].read().decode('utf-8'))
            metrics.source_url = metadata.get('url')
        except Exception:
            pass
        
        # Save to knowledge store
        self.knowledge_store.save_rate_notice(metrics)
        
        return metrics
    
    def process_all_rate_notices(self, years: List[int] = None):
        """Process all available rate notices."""
        if years is None:
            years = list(range(2016, 2028))
        
        results = []
        for year in years:
            for notice_type in ['advance', 'final']:
                print(f"Processing {notice_type} notice for {year}...")
                try:
                    metrics = self.process_rate_notice(year, notice_type)
                    if metrics:
                        results.append(metrics)
                        print(f"  ✓ Extracted {notice_type} {year}")
                except Exception as e:
                    print(f"  ✗ Failed: {e}")
        
        return results
    
    def build_hcc_model_knowledge(self):
        """Build HCC model knowledge from known information."""
        # These are known facts about HCC model versions
        models = [
            HCCModelParameters(
                model_version="V28",
                model_year=2024,
                phasein_pct=33.0,
                blended_with="V24",
                blended_pct=67.0,
                total_hccs=115,
                segments=["Community Non-Dual Aged", "Community Non-Dual Disabled", 
                         "Community Full Benefit Dual Aged", "Community Full Benefit Dual Disabled",
                         "Institutional", "New Enrollee"],
                changes_from_prior=[
                    "Reduced from 86 HCCs to 115 HCCs (consolidated similar conditions)",
                    "Added new HCCs for substance use disorders",
                    "Updated coefficients based on 2020-2021 data",
                    "Removed some lower-severity conditions",
                ]
            ),
            HCCModelParameters(
                model_version="V28",
                model_year=2025,
                phasein_pct=67.0,
                blended_with="V24",
                blended_pct=33.0,
                total_hccs=115,
                segments=["Community Non-Dual Aged", "Community Non-Dual Disabled", 
                         "Community Full Benefit Dual Aged", "Community Full Benefit Dual Disabled",
                         "Institutional", "New Enrollee"],
            ),
            HCCModelParameters(
                model_version="V28",
                model_year=2026,
                phasein_pct=100.0,
                blended_with=None,
                blended_pct=0.0,
                total_hccs=115,
                segments=["Community Non-Dual Aged", "Community Non-Dual Disabled", 
                         "Community Full Benefit Dual Aged", "Community Full Benefit Dual Disabled",
                         "Institutional", "New Enrollee"],
            ),
            HCCModelParameters(
                model_version="V28",
                model_year=2027,
                phasein_pct=100.0,
                blended_with=None,
                blended_pct=0.0,
                total_hccs=115,
                segments=["Community Non-Dual Aged", "Community Non-Dual Disabled", 
                         "Community Full Benefit Dual Aged", "Community Full Benefit Dual Disabled",
                         "Institutional", "New Enrollee"],
            ),
            HCCModelParameters(
                model_version="V24",
                model_year=2023,
                phasein_pct=100.0,
                total_hccs=86,
                segments=["Community Aged", "Community Disabled", "Institutional", "New Enrollee"],
            ),
        ]
        
        for model in models:
            self.knowledge_store.save_hcc_model(model)
            print(f"Saved HCC model {model.model_version} for {model.model_year}")
    
    def build_policy_changes_knowledge(self):
        """Build policy changes knowledge from known information."""
        changes = [
            PolicyChange(
                change_id="ira_oop_cap_2025",
                effective_year=2025,
                category="part_d",
                title="Part D Out-of-Pocket Cap",
                description="$2,000 annual cap on Part D out-of-pocket spending (IRA provision)",
                impact_level="high",
                affected_entities=["Part D sponsors", "MA-PD plans", "Beneficiaries"],
                source_document="Inflation Reduction Act of 2022",
            ),
            PolicyChange(
                change_id="ira_insulin_cap",
                effective_year=2024,
                category="part_d",
                title="$35 Insulin Cap",
                description="Monthly cost-sharing for insulin capped at $35 (IRA provision)",
                impact_level="high",
                affected_entities=["Part D sponsors", "MA-PD plans", "Beneficiaries with diabetes"],
                source_document="Inflation Reduction Act of 2022",
            ),
            PolicyChange(
                change_id="v28_full_implementation",
                effective_year=2026,
                category="risk_adjustment",
                title="V28 Risk Adjustment Model Full Implementation",
                description="CMS-HCC V28 model used at 100% (no blending with V24)",
                impact_level="high",
                affected_entities=["MA plans", "Providers"],
                source_document="2024 Final Rule",
            ),
            PolicyChange(
                change_id="star_measure_updates_2026",
                effective_year=2026,
                category="star_ratings",
                title="Star Ratings Measure Updates",
                description="New and modified measures for 2026 star ratings",
                impact_level="medium",
                affected_entities=["MA plans", "MA-PD plans"],
                source_document="2026 Star Ratings Technical Notes",
            ),
        ]
        
        for change in changes:
            self.knowledge_store.add_policy_change(change)
            print(f"Added policy change: {change.title}")


# =============================================================================
# SINGLETON ACCESS
# =============================================================================

_knowledge_store: Optional[MAKnowledgeStore] = None
_extraction_pipeline: Optional[KnowledgeExtractionPipeline] = None


def get_knowledge_store() -> MAKnowledgeStore:
    """Get or create singleton knowledge store."""
    global _knowledge_store
    if _knowledge_store is None:
        _knowledge_store = MAKnowledgeStore()
    return _knowledge_store


def get_extraction_pipeline() -> KnowledgeExtractionPipeline:
    """Get or create singleton extraction pipeline."""
    global _extraction_pipeline
    if _extraction_pipeline is None:
        _extraction_pipeline = KnowledgeExtractionPipeline()
    return _extraction_pipeline


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="MA Knowledge Extraction Pipeline")
    parser.add_argument("--extract-rate-notices", action="store_true", help="Extract all rate notices")
    parser.add_argument("--build-hcc-models", action="store_true", help="Build HCC model knowledge")
    parser.add_argument("--build-policy-changes", action="store_true", help="Build policy changes")
    parser.add_argument("--year", type=int, help="Specific year to process")
    parser.add_argument("--all", action="store_true", help="Run full extraction")
    
    args = parser.parse_args()
    
    pipeline = get_extraction_pipeline()
    
    if args.all or args.extract_rate_notices:
        years = [args.year] if args.year else None
        pipeline.process_all_rate_notices(years)
    
    if args.all or args.build_hcc_models:
        pipeline.build_hcc_model_knowledge()
    
    if args.all or args.build_policy_changes:
        pipeline.build_policy_changes_knowledge()
    
    print("\nKnowledge extraction complete!")
