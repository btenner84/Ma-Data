const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

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

// Enrollment API
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

// Stars API
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
  enrollment_source: string;  // e.g., "2026/01"
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

// Contract API
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

// Risk Score API
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

// V2 Risk Score Types
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

  // V2 API methods
  getFiltersV2: () =>
    fetchAPI<RiskScoreFilters>("/api/v2/risk-scores/filters"),

  getSummaryV2: (year?: number) =>
    fetchAPI<RiskScoreSummaryV2>(
      `/api/v2/risk-scores/summary${year ? `?year=${year}` : ""}`
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
      `/api/v2/risk-scores/timeseries?${searchParams}`
    );
  },
};

// Lookup API
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
