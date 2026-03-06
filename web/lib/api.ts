const API_BASE = (process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000").replace(/\/$/, "");

async function fetchAPI<T>(endpoint: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${endpoint}`, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options?.headers,
    },
  });

  if (!response.ok) {
    throw new Error(`API error: ${response.status} ${response.statusText}`);
  }

  return response.json();
}

// =============================================================================
// V3 TYPES - With Audit Support
// =============================================================================

// Base response with audit ID for lineage tracing
export interface AuditedResponse {
  audit_id: string;
}

// V3 Filter options
export interface V3StarsFilters {
  parent_orgs: string[];
  plan_types: string[];
  group_types: string[];
  snp_types: string[];
  star_years: number[];
  states: string[];
  measure_ids: string[];
  domains: string[];
  parts: string[];
  data_sources: string[];
  audit_ids: string[];
}

export interface V3RiskFilters {
  years: number[];
  parent_orgs: string[];
  plan_types: string[];
  group_types: string[];
  snp_types: string[];
  states: string[];
  audit_ids: string[];
}

// V3 Stars Distribution
export interface V3StarsDistribution extends AuditedResponse {
  years: number[];
  series: Record<string, (number | null)[]>;
  data: {
    star_year: number;
    parent_org: string;
    enrollment: number;
    fourplus_enrollment: number;
    fourplus_pct: number;
    contract_count: number;
  }[];
  filters: {
    parent_orgs: string[] | null;
    plan_types: string[] | null;
    group_types: string[] | null;
    snp_types: string[] | null;
    states: string[] | null;
    star_year: number | null;
  };
}

// V3 Stars By Parent
export interface V3StarsByParent extends AuditedResponse {
  star_year: number;
  data: {
    parent_org: string;
    total_enrollment: number;
    fourplus_enrollment: number;
    fourplus_pct: number;
    wavg_rating: number;
    contract_count: number;
  }[];
  filters: Record<string, any>;
}

// V3 Stars By State
export interface V3StarsByState extends AuditedResponse {
  star_year: number;
  data: {
    state: string;
    total_enrollment: number;
    fourplus_enrollment: number;
    fourplus_pct: number;
    wavg_rating: number;
    parent_count: number;
  }[];
  filters: Record<string, any>;
}

// V3 Risk Timeseries
export interface V3RiskTimeseries extends AuditedResponse {
  years: number[];
  series: Record<string, (number | null)[]>;
  enrollment: Record<string, (number | null)[]>;
  metric: "avg" | "wavg";
  group_by: string | null;
  filters: {
    parent_orgs: string[] | null;
    plan_types: string[] | null;
    group_types: string[] | null;
    snp_types: string[] | null;
    states: string[] | null;
  };
}

// V3 Risk By Parent
export interface V3RiskByParent extends AuditedResponse {
  year: number;
  data: {
    parent_org: string;
    total_enrollment: number;
    contract_count: number;
    simple_avg_risk_score: number;
    wavg_risk_score: number;
    min_risk_score: number;
    max_risk_score: number;
  }[];
  filters: Record<string, any>;
}

// V3 Risk By State
export interface V3RiskByState extends AuditedResponse {
  year: number;
  data: {
    state: string;
    total_enrollment: number;
    parent_count: number;
    wavg_risk_score: number;
  }[];
  filters: Record<string, any>;
}

// V3 Risk By Dimensions
export interface V3RiskByDimensions extends AuditedResponse {
  year: number;
  data: {
    plan_type: string;
    snp_type: string;
    group_type: string;
    enrollment: number;
    contract_count: number;
    simple_avg: number;
    wavg: number;
  }[];
  filters: Record<string, any>;
}

// V3 Enrollment Timeseries
export interface V3EnrollmentTimeseries extends AuditedResponse {
  years: number[];
  total_enrollment: number[];
  data: {
    year: number;
    enrollment: number;
    plan_count: number;
    parent_org_count?: number;
    yoy_growth?: number;
  }[];
  filters: Record<string, any>;
}

// V3 Enrollment By Parent
export interface V3EnrollmentByParent extends AuditedResponse {
  year: number;
  total_enrollment: number;
  data: {
    parent_org: string;
    total_enrollment: number;
    plan_count: number;
    contract_count: number;
    market_share: number;
  }[];
}

// =============================================================================
// V3 API - STARS (with audit/lineage)
// =============================================================================

export const starsAPIv3 = {
  // Get all filter options
  getFilters: () =>
    fetchAPI<V3StarsFilters>("/api/v3/stars/filters"),

  // Get 4+ star distribution timeseries
  getDistribution: (params: {
    parentOrgs?: string[];
    planTypes?: string[];
    groupTypes?: string[];
    snpTypes?: string[];
    states?: string[];
    starYear?: number;
    includeIndustryTotal?: boolean;
  } = {}) => {
    const searchParams = new URLSearchParams();
    if (params.parentOrgs?.length) {
      searchParams.set("parent_orgs", params.parentOrgs.join("|"));
    }
    if (params.planTypes?.length) {
      searchParams.set("plan_types", params.planTypes.join(","));
    }
    if (params.groupTypes?.length) {
      searchParams.set("group_types", params.groupTypes.join(","));
    }
    if (params.snpTypes?.length) {
      searchParams.set("snp_types", params.snpTypes.join(","));
    }
    if (params.states?.length) {
      searchParams.set("states", params.states.join(","));
    }
    if (params.starYear) {
      searchParams.set("star_year", params.starYear.toString());
    }
    if (params.includeIndustryTotal !== undefined) {
      searchParams.set("include_industry_total", params.includeIndustryTotal.toString());
    }
    return fetchAPI<V3StarsDistribution>(`/api/v3/stars/distribution?${searchParams}`);
  },

  // Get stars by parent organization
  getByParent: (params: {
    starYear?: number;
    planTypes?: string[];
    groupTypes?: string[];
    snpTypes?: string[];
    states?: string[];
    limit?: number;
  } = {}) => {
    const searchParams = new URLSearchParams();
    if (params.starYear) searchParams.set("star_year", params.starYear.toString());
    if (params.planTypes?.length) searchParams.set("plan_types", params.planTypes.join(","));
    if (params.groupTypes?.length) searchParams.set("group_types", params.groupTypes.join(","));
    if (params.snpTypes?.length) searchParams.set("snp_types", params.snpTypes.join(","));
    if (params.states?.length) searchParams.set("states", params.states.join(","));
    if (params.limit) searchParams.set("limit", params.limit.toString());
    return fetchAPI<V3StarsByParent>(`/api/v3/stars/by-parent?${searchParams}`);
  },

  // Get stars by state
  getByState: (params: {
    starYear?: number;
    planTypes?: string[];
    parentOrgs?: string[];
  } = {}) => {
    const searchParams = new URLSearchParams();
    if (params.starYear) searchParams.set("star_year", params.starYear.toString());
    if (params.planTypes?.length) searchParams.set("plan_types", params.planTypes.join(","));
    if (params.parentOrgs?.length) searchParams.set("parent_orgs", params.parentOrgs.join("|"));
    return fetchAPI<V3StarsByState>(`/api/v3/stars/by-state?${searchParams}`);
  },

  // Get measure performance
  getMeasures: (params: {
    year?: number;
    parentOrgs?: string[];
    measureIds?: string[];
    domains?: string[];
    parts?: string[];
    dataSources?: string[];
  } = {}) => {
    const searchParams = new URLSearchParams();
    if (params.year) searchParams.set("year", params.year.toString());
    if (params.parentOrgs?.length) searchParams.set("parent_orgs", params.parentOrgs.join("|"));
    if (params.measureIds?.length) searchParams.set("measure_ids", params.measureIds.join(","));
    if (params.domains?.length) searchParams.set("domains", params.domains.join(","));
    if (params.parts?.length) searchParams.set("parts", params.parts.join(","));
    if (params.dataSources?.length) searchParams.set("data_sources", params.dataSources.join(","));
    return fetchAPI<AuditedResponse & { year: number; data: any[] }>(`/api/v3/stars/measures?${searchParams}`);
  },

  // Get cutpoints
  getCutpoints: (params: {
    years?: number[];
    measureIds?: string[];
    parts?: string[];
  } = {}) => {
    const searchParams = new URLSearchParams();
    if (params.years?.length) searchParams.set("years", params.years.join(","));
    if (params.measureIds?.length) searchParams.set("measure_ids", params.measureIds.join(","));
    if (params.parts?.length) searchParams.set("parts", params.parts.join(","));
    return fetchAPI<AuditedResponse & { data: any[] }>(`/api/v3/stars/cutpoints?${searchParams}`);
  },

  // Get contract detail
  getContract: (contractId: string, year: number = 2026) =>
    fetchAPI<AuditedResponse & { contract_id: string; year: number; summary: any; measures: any[] }>(
      `/api/v3/stars/contract/${contractId}?year=${year}`
    ),
};

// =============================================================================
// V3 API - RISK SCORES (with audit/lineage)
// =============================================================================

export const riskAPIv3 = {
  // Get all filter options
  getFilters: () =>
    fetchAPI<V3RiskFilters>("/api/v3/risk/filters"),

  // Get summary statistics
  getSummary: (params: {
    year?: number;
    parentOrgs?: string[];
    planTypes?: string[];
    groupTypes?: string[];
    snpTypes?: string[];
    states?: string[];
  } = {}) => {
    const searchParams = new URLSearchParams();
    if (params.year) searchParams.set("year", params.year.toString());
    if (params.parentOrgs?.length) searchParams.set("parent_orgs", params.parentOrgs.join("|"));
    if (params.planTypes?.length) searchParams.set("plan_types", params.planTypes.join(","));
    if (params.groupTypes?.length) searchParams.set("group_types", params.groupTypes.join(","));
    if (params.snpTypes?.length) searchParams.set("snp_types", params.snpTypes.join(","));
    if (params.states?.length) searchParams.set("states", params.states.join(","));
    return fetchAPI<AuditedResponse & { data: any[] }>(`/api/v3/risk/summary?${searchParams}`);
  },

  // Get risk score timeseries
  getTimeseries: (params: {
    parentOrgs?: string[];
    planTypes?: string[];
    groupTypes?: string[];
    snpTypes?: string[];
    states?: string[];
    metric?: "avg" | "wavg";
    includeIndustryTotal?: boolean;
    groupBy?: "plan_type" | "snp_type" | "group_type";
  } = {}) => {
    const searchParams = new URLSearchParams();
    if (params.parentOrgs?.length) searchParams.set("parent_orgs", params.parentOrgs.join("|"));
    if (params.planTypes?.length) searchParams.set("plan_types", params.planTypes.join(","));
    if (params.groupTypes?.length) searchParams.set("group_types", params.groupTypes.join(","));
    if (params.snpTypes?.length) searchParams.set("snp_types", params.snpTypes.join(","));
    if (params.states?.length) searchParams.set("states", params.states.join(","));
    if (params.metric) searchParams.set("metric", params.metric);
    if (params.includeIndustryTotal !== undefined) {
      searchParams.set("include_industry_total", params.includeIndustryTotal.toString());
    }
    if (params.groupBy) searchParams.set("group_by", params.groupBy);
    return fetchAPI<V3RiskTimeseries>(`/api/v3/risk/timeseries?${searchParams}`);
  },

  // Get risk by parent organization
  getByParent: (params: {
    year?: number;
    planTypes?: string[];
    groupTypes?: string[];
    snpTypes?: string[];
    states?: string[];
    limit?: number;
  } = {}) => {
    const searchParams = new URLSearchParams();
    if (params.year) searchParams.set("year", params.year.toString());
    if (params.planTypes?.length) searchParams.set("plan_types", params.planTypes.join(","));
    if (params.groupTypes?.length) searchParams.set("group_types", params.groupTypes.join(","));
    if (params.snpTypes?.length) searchParams.set("snp_types", params.snpTypes.join(","));
    if (params.states?.length) searchParams.set("states", params.states.join(","));
    if (params.limit) searchParams.set("limit", params.limit.toString());
    return fetchAPI<V3RiskByParent>(`/api/v3/risk/by-parent?${searchParams}`);
  },

  // Get risk by state
  getByState: (params: {
    year?: number;
    planTypes?: string[];
    parentOrgs?: string[];
  } = {}) => {
    const searchParams = new URLSearchParams();
    if (params.year) searchParams.set("year", params.year.toString());
    if (params.planTypes?.length) searchParams.set("plan_types", params.planTypes.join(","));
    if (params.parentOrgs?.length) searchParams.set("parent_orgs", params.parentOrgs.join("|"));
    return fetchAPI<V3RiskByState>(`/api/v3/risk/by-state?${searchParams}`);
  },

  // Get risk by dimensions
  getByDimensions: (params: {
    year?: number;
    parentOrgs?: string[];
    states?: string[];
  } = {}) => {
    const searchParams = new URLSearchParams();
    if (params.year) searchParams.set("year", params.year.toString());
    if (params.parentOrgs?.length) searchParams.set("parent_orgs", params.parentOrgs.join("|"));
    if (params.states?.length) searchParams.set("states", params.states.join(","));
    return fetchAPI<V3RiskByDimensions>(`/api/v3/risk/by-dimensions?${searchParams}`);
  },

  // Get risk distribution (histogram)
  getDistribution: (params: {
    year?: number;
    parentOrgs?: string[];
    planTypes?: string[];
    bins?: number;
  } = {}) => {
    const searchParams = new URLSearchParams();
    if (params.year) searchParams.set("year", params.year.toString());
    if (params.parentOrgs?.length) searchParams.set("parent_orgs", params.parentOrgs.join("|"));
    if (params.planTypes?.length) searchParams.set("plan_types", params.planTypes.join(","));
    if (params.bins) searchParams.set("bins", params.bins.toString());
    return fetchAPI<AuditedResponse & { year: number; distribution: any[] }>(`/api/v3/risk/distribution?${searchParams}`);
  },

  // Get plan detail
  getPlan: (contractId: string, planId: string) =>
    fetchAPI<AuditedResponse & { contract_id: string; plan_id: string; history: any[] }>(
      `/api/v3/risk/plan/${contractId}/${planId}`
    ),
};

// =============================================================================
// V3 API - ENROLLMENT (with audit/lineage)
// =============================================================================

export const enrollmentAPIv3 = {
  // Get enrollment timeseries
  getTimeseries: (params: {
    parentOrg?: string;
    state?: string;
    planType?: string;
    productType?: string;
    groupType?: string;
    snpType?: string;
    startYear?: number;
    endYear?: number;
  } = {}) => {
    const searchParams = new URLSearchParams();
    if (params.parentOrg) searchParams.set("parent_org", params.parentOrg);
    if (params.state) searchParams.set("state", params.state);
    if (params.planType) searchParams.set("plan_type", params.planType);
    if (params.productType) searchParams.set("product_type", params.productType);
    if (params.groupType) searchParams.set("group_type", params.groupType);
    if (params.snpType) searchParams.set("snp_type", params.snpType);
    if (params.startYear) searchParams.set("start_year", params.startYear.toString());
    if (params.endYear) searchParams.set("end_year", params.endYear.toString());
    return fetchAPI<V3EnrollmentTimeseries>(`/api/v3/enrollment/timeseries?${searchParams}`);
  },

  // Get enrollment by parent
  getByParent: (params: {
    year?: number;
    month?: number;
    limit?: number;
  } = {}) => {
    const searchParams = new URLSearchParams();
    if (params.year) searchParams.set("year", params.year.toString());
    if (params.month) searchParams.set("month", params.month.toString());
    if (params.limit) searchParams.set("limit", params.limit.toString());
    return fetchAPI<V3EnrollmentByParent>(`/api/v3/enrollment/by-parent?${searchParams}`);
  },

  // Get enrollment by state
  getByState: (params: {
    year?: number;
    month?: number;
  } = {}) => {
    const searchParams = new URLSearchParams();
    if (params.year) searchParams.set("year", params.year.toString());
    if (params.month) searchParams.set("month", params.month.toString());
    return fetchAPI<AuditedResponse & { year: number; data: any[] }>(`/api/v3/enrollment/by-state?${searchParams}`);
  },

  // Get enrollment by dimensions
  getByDimensions: (params: {
    year?: number;
    month?: number;
    planType?: string;
    productType?: string;
    groupType?: string;
    snpType?: string;
  } = {}) => {
    const searchParams = new URLSearchParams();
    if (params.year) searchParams.set("year", params.year.toString());
    if (params.month) searchParams.set("month", params.month.toString());
    if (params.planType) searchParams.set("plan_type", params.planType);
    if (params.productType) searchParams.set("product_type", params.productType);
    if (params.groupType) searchParams.set("group_type", params.groupType);
    if (params.snpType) searchParams.set("snp_type", params.snpType);
    return fetchAPI<AuditedResponse & { data: any[] }>(`/api/v3/enrollment/by-dimensions?${searchParams}`);
  },
};

// =============================================================================
// V3 API - LINEAGE
// =============================================================================

export interface LineageResponse {
  audit_id: string;
  query_timestamp: string;
  tables_accessed: string[];
  tables_lineage: Record<string, {
    primary_sources: string[];
    build_script: string;
    grain: string;
  }>;
  full_source_chain: string[];
  error?: string;
}

export const lineageAPI = {
  // Trace lineage for any audit ID
  trace: (auditId: string) =>
    fetchAPI<LineageResponse>(`/api/v3/lineage/${auditId}`),
};

// =============================================================================
// LEGACY API - Keep for backwards compatibility
// =============================================================================

// Enrollment API (legacy v1)
export interface EnrollmentTimeSeries {
  years: number[];
  total_enrollment: number[];
  contract_count: number[];
  plan_count: number[];
}

export interface ParentEnrollment {
  parent_org: string;
  total_enrollment: number;
  contract_count: number;
  plan_count: number;
  county_count: number;
}

export interface MarketShare {
  parent_org: string;
  total_enrollment: number;
  market_share: number;
}

export interface EnrollmentByPlanType {
  plan_type: string;
  total_enrollment: number;
  plan_count: number;
}

export interface EnrollmentByState {
  state: string;
  total_enrollment: number;
  plan_count: number;
}

export const enrollmentAPI = {
  getTimeSeries: (parentOrg?: string) =>
    fetchAPI<EnrollmentTimeSeries>(
      `/api/enrollment/timeseries${parentOrg ? `?parent_org=${encodeURIComponent(parentOrg)}` : ""}`
    ),

  getByParent: (year?: number, topN = 20) =>
    fetchAPI<{ year: number; data: ParentEnrollment[] }>(
      `/api/enrollment/by-parent?top_n=${topN}${year ? `&year=${year}` : ""}`
    ),

  getMarketShare: (year?: number) =>
    fetchAPI<{ year: number; total_enrollment: number; data: MarketShare[] }>(
      `/api/enrollment/market-share${year ? `?year=${year}` : ""}`
    ),

  getByPlanType: (year?: number) =>
    fetchAPI<{ year: number; data: EnrollmentByPlanType[] }>(
      `/api/enrollment/by-plan-type${year ? `?year=${year}` : ""}`
    ),

  getByState: (year?: number) =>
    fetchAPI<{ year: number; data: EnrollmentByState[] }>(
      `/api/enrollment/by-state${year ? `?year=${year}` : ""}`
    ),
};

// Stars API (legacy v1)
export interface StarsBand {
  year: number;
  bands: Record<string, number>;
}

export interface MeasureSummary {
  measure_id: string;
  avg_rating: number;
  rating_count: number;
  contract_count: number;
}

export interface StarsByPlanType {
  plan_type: string;
  avg_rating: number;
  rating_count: number;
  contract_count: number;
}

export interface StarsDistributionBand {
  enrollment: number;
  contracts: number;
  pct: number;
}

export interface StarsDistributionColumn {
  total_enrollment: number;
  distribution: Record<number, StarsDistributionBand>;
}

export interface StarsDistributionResponse {
  star_year: number;
  payment_year: number;
  enrollment_source: string;
  total_enrollment: number;
  total_contracts: number;
  filters: {
    plan_types: string;
  };
  columns: Record<string, StarsDistributionColumn>;
  error?: string;
}

export const starsAPI = {
  getSummary: (year?: number) =>
    fetchAPI<{ years: number[]; total_contracts: number; data: any[] }>(
      `/api/stars/summary${year ? `?year=${year}` : ""}`
    ),

  getByBand: (year?: number) =>
    fetchAPI<StarsBand>(`/api/stars/by-band${year ? `?year=${year}` : ""}`),

  getDistribution: (parentOrgs?: string[], starYear?: number) => {
    const params = new URLSearchParams();
    if (starYear) params.set("star_year", starYear.toString());
    if (parentOrgs && parentOrgs.length > 0) {
      params.set("parent_orgs", parentOrgs.join("|"));
    }
    return fetchAPI<StarsDistributionResponse>(
      `/api/stars/distribution?${params}`
    );
  },

  getMeasures: (year?: number, contractId?: string, measureId?: string) => {
    const params = new URLSearchParams();
    if (year) params.set("year", year.toString());
    if (contractId) params.set("contract_id", contractId);
    if (measureId) params.set("measure_id", measureId);
    return fetchAPI<{ count: number; data: any[] }>(
      `/api/stars/measures?${params}`
    );
  },

  getMeasureSummary: (year?: number) =>
    fetchAPI<{ year: number; measures: MeasureSummary[] }>(
      `/api/stars/measure-summary${year ? `?year=${year}` : ""}`
    ),

  getByPlanType: (year?: number) =>
    fetchAPI<{ year: number; data: StarsByPlanType[] }>(
      `/api/stars/by-plan-type${year ? `?year=${year}` : ""}`
    ),
};

// Contract API (legacy)
export const contractAPI = {
  getDetail: (contractId: string, year?: number) =>
    fetchAPI<{ contract_id: string; stars_data: any[]; measures: any[] }>(
      `/api/contract/${contractId}${year ? `?year=${year}` : ""}`
    ),

  getParentDetail: (parentOrg: string) =>
    fetchAPI<{
      parent_org: string;
      contracts: string[];
      enrollment_history: any[];
      ratings_history: Record<string, number>;
    }>(`/api/parent/${encodeURIComponent(parentOrg)}`),
};

// Risk Score API (legacy v1/v2)
export interface RiskScoreSummary {
  year: number;
  record_count: number;
  mean_risk_score: number;
  min_risk_score: number;
  max_risk_score: number;
  error?: string;
}

export interface RiskScoreTimeSeries {
  years: number[];
  avg_risk_score: number[];
  record_count: number[];
  error?: string;
}

export interface PlanTypeRiskScore {
  "Plan Type": string;
  avg_risk_score: number;
  min_risk_score: number;
  max_risk_score: number;
  count: number;
}

export interface StateRiskScore {
  "State Abbreviation": string;
  avg_risk_score: number;
  count: number;
}

export interface RiskScoreSummaryV2 {
  year: number;
  record_count: number;
  contract_count: number;
  mean_risk_score: number;
  wavg_risk_score: number | null;
  min_risk_score: number;
  max_risk_score: number;
  std_risk_score: number | null;
  total_enrollment: number;
  error?: string;
}

export interface RiskScoreFilters {
  years: number[];
  parent_orgs: string[];
  plan_types: string[];
  plan_types_simplified: string[];
  snp_types: string[];
  group_types: string[];
}

export interface RiskScoreTimeSeriesV2 {
  years: number[];
  series: Record<string, (number | null)[]>;
  enrollment: Record<string, (number | null)[]>;
  metric: "avg" | "wavg";
  group_by: string | null;
  error?: string;
}

export const riskScoreAPI = {
  getSummary: (year?: number) =>
    fetchAPI<RiskScoreSummary>(
      `/api/risk-scores/summary${year ? `?year=${year}` : ""}`
    ),

  getTimeSeries: () =>
    fetchAPI<RiskScoreTimeSeries>("/api/risk-scores/timeseries"),

  getDistribution: (year?: number, planType?: string) => {
    const params = new URLSearchParams();
    if (year) params.set("year", year.toString());
    if (planType) params.set("plan_type", planType);
    return fetchAPI<{ year: number; plan_type: string; distribution: Record<string, number> }>(
      `/api/risk-scores/distribution?${params}`
    );
  },

  getByPlanType: () =>
    fetchAPI<{ year: number; data: PlanTypeRiskScore[] }>("/api/risk-scores/by-plan-type"),

  getByState: () =>
    fetchAPI<{ year: number; data: StateRiskScore[] }>("/api/risk-scores/by-state"),

  // V2 API methods (now pointing to V3 for audit/lineage)
  getFiltersV2: () =>
    fetchAPI<RiskScoreFilters>("/api/v3/risk/filters"),

  getSummaryV2: (year?: number) =>
    fetchAPI<RiskScoreSummaryV2>(
      `/api/v3/risk/summary${year ? `?year=${year}` : ""}`
    ),

  getTimeSeriesV2: (params: {
    parentOrgs?: string[];
    planTypes?: string[];
    snpTypes?: string[];
    groupTypes?: string[];
    groupBy?: string;
    includeTotal?: boolean;
    metric?: "avg" | "wavg";
  }) => {
    const searchParams = new URLSearchParams();
    if (params.parentOrgs?.length) {
      searchParams.set("parent_orgs", params.parentOrgs.join("|"));
    }
    if (params.planTypes?.length) {
      searchParams.set("plan_types", params.planTypes.join(","));
    }
    if (params.snpTypes?.length) {
      searchParams.set("snp_types", params.snpTypes.join(","));
    }
    if (params.groupTypes?.length) {
      searchParams.set("group_types", params.groupTypes.join(","));
    }
    if (params.groupBy) {
      searchParams.set("group_by", params.groupBy);
    }
    if (params.includeTotal !== undefined) {
      searchParams.set("include_total", params.includeTotal.toString());
    }
    if (params.metric) {
      searchParams.set("metric", params.metric);
    }
    return fetchAPI<RiskScoreTimeSeriesV2>(
      `/api/v3/risk/timeseries?${searchParams}`
    );
  },
};

// Lookup API (legacy)
export const lookupAPI = {
  getParents: () =>
    fetchAPI<{ count: number; parents: { parent_org: string; total_enrollment: number }[] }>(
      "/api/lookup/parents"
    ),

  getYears: () =>
    fetchAPI<{
      enrollment_years: number[];
      stars_years: number[];
      measure_years: number[];
    }>("/api/lookup/years"),
};
