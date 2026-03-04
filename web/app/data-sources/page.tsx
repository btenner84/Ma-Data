"use client";

import Link from "next/link";
import { Home, Database, Download, FileText, Users, Star, TrendingUp, AlertTriangle, ChevronRight, ExternalLink } from "lucide-react";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

interface DataSourceCard {
  id: string;
  name: string;
  description: string;
  icon: React.ReactNode;
  color: string;
  files: {
    name: string;
    description: string;
    years: string;
    downloadUrl?: string;
    cmsUrl?: string;
  }[];
}

const dataSources: DataSourceCard[] = [
  {
    id: "cpsc",
    name: "CPSC Enrollment",
    description: "Monthly Enrollment by Contract-Plan-State-County. The most granular enrollment data with geographic detail. Some values suppressed for HIPAA (<10 enrollees).",
    icon: <Users className="w-6 h-6" />,
    color: "blue",
    files: [
      { name: "CPSC Monthly Files", description: "Contract/Plan/State/County enrollment counts", years: "2007-2026", cmsUrl: "https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/monthly-enrollment-contract-plan-state-county" },
    ]
  },
  {
    id: "enrollment-by-plan",
    name: "Enrollment by Plan",
    description: "Monthly enrollment at contract-plan level with parent organization. No geographic detail but includes all enrollment (no suppression).",
    icon: <FileText className="w-6 h-6" />,
    color: "green",
    files: [
      { name: "Monthly Enrollment by Plan", description: "Contract/Plan/Parent Org enrollment", years: "2013-2026", cmsUrl: "https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/monthly-enrollment-plan" },
    ]
  },
  {
    id: "special-needs",
    name: "Special Needs Plans (SNP)",
    description: "Enrollment data for D-SNP (Dual), C-SNP (Chronic), and I-SNP (Institutional) plans with state-level detail.",
    icon: <AlertTriangle className="w-6 h-6" />,
    color: "purple",
    files: [
      { name: "SNP Comprehensive Report", description: "D-SNP, C-SNP, I-SNP enrollment by state", years: "2010-2026", cmsUrl: "https://www.cms.gov/data-research/statistics-trends-and-reports/medicare-advantagepart-d-contract-and-enrollment-data/special-needs-plan-snp-data" },
    ]
  },
  {
    id: "stars-overall",
    name: "Star Ratings - Overall",
    description: "Overall star ratings (1-5 stars) for each MA contract. Used for quality bonus payments and enrollment decisions.",
    icon: <Star className="w-6 h-6" />,
    color: "yellow",
    files: [
      { name: "Star Ratings Data", description: "Overall, Part C, Part D ratings by contract", years: "2009-2026", cmsUrl: "https://www.cms.gov/medicare/health-drug-plans/part-c-d-performance-data" },
      { name: "Display Measures", description: "Contract-level performance on display measures", years: "2015-2026", downloadUrl: `${API_BASE}/api/stars/export?year=2026&format=xlsx` },
    ]
  },
  {
    id: "stars-measures",
    name: "Star Ratings - Measures",
    description: "Performance on individual quality measures (HEDIS, CAHPS, HOS). ~40 measures for Part C, ~15 for Part D.",
    icon: <Star className="w-6 h-6" />,
    color: "orange",
    files: [
      { name: "Part C & D Measure Data", description: "Performance % on each quality measure", years: "2019-2026", cmsUrl: "https://www.cms.gov/medicare/health-drug-plans/part-c-d-performance-data" },
    ]
  },
  {
    id: "cutpoints",
    name: "Star Cutpoints",
    description: "Performance thresholds that determine star ratings. Updated annually by CMS. Critical for predicting future ratings.",
    icon: <TrendingUp className="w-6 h-6" />,
    color: "teal",
    files: [
      { name: "Technical Notes (PDFs)", description: "Contains cutpoint tables for each year", years: "2019-2026", cmsUrl: "https://www.cms.gov/medicare/health-drug-plans/part-c-d-performance-data" },
      { name: "Cutpoints Export", description: "All cutpoints in structured format", years: "2019-2026", downloadUrl: `${API_BASE}/api/stars/cutpoints-export?format=xlsx` },
    ]
  },
  {
    id: "risk-scores",
    name: "Risk Adjustment Scores",
    description: "CMS risk scores by contract/plan. Higher scores = sicker population = higher payments. Critical for understanding plan economics.",
    icon: <TrendingUp className="w-6 h-6" />,
    color: "red",
    files: [
      { name: "Plan Payment Data", description: "Risk scores, benchmarks, rebates by contract", years: "2007-2026", cmsUrl: "https://www.cms.gov/medicare/payment/medicare-advantage-rates-statistics/plan-payment-data" },
    ]
  },
  {
    id: "crosswalk",
    name: "Contract Crosswalk",
    description: "Tracks contract ID changes over time (mergers, acquisitions, rebranding). Essential for longitudinal analysis.",
    icon: <Database className="w-6 h-6" />,
    color: "gray",
    files: [
      { name: "Contract Crosswalk", description: "Old contract → New contract mappings", years: "2007-2026", cmsUrl: "https://www.cms.gov/medicare/enrollment-renewal/contract-crosswalk" },
    ]
  },
];

const colorClasses: Record<string, { bg: string; border: string; text: string; light: string }> = {
  blue: { bg: "bg-blue-500", border: "border-blue-200", text: "text-blue-700", light: "bg-blue-50" },
  green: { bg: "bg-green-500", border: "border-green-200", text: "text-green-700", light: "bg-green-50" },
  purple: { bg: "bg-purple-500", border: "border-purple-200", text: "text-purple-700", light: "bg-purple-50" },
  yellow: { bg: "bg-yellow-500", border: "border-yellow-200", text: "text-yellow-700", light: "bg-yellow-50" },
  orange: { bg: "bg-orange-500", border: "border-orange-200", text: "text-orange-700", light: "bg-orange-50" },
  teal: { bg: "bg-teal-500", border: "border-teal-200", text: "text-teal-700", light: "bg-teal-50" },
  red: { bg: "bg-red-500", border: "border-red-200", text: "text-red-700", light: "bg-red-50" },
  gray: { bg: "bg-gray-500", border: "border-gray-200", text: "text-gray-700", light: "bg-gray-50" },
};

export default function DataSourcesPage() {
  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200 sticky top-0 z-30">
        <div className="max-w-7xl mx-auto px-6 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              <Link href="/" className="flex items-center gap-2 text-gray-600 hover:text-gray-900">
                <Home className="w-5 h-5" />
              </Link>
              <ChevronRight className="w-4 h-4 text-gray-400" />
              <div className="flex items-center gap-2">
                <Database className="w-5 h-5 text-blue-600" />
                <h1 className="text-xl font-bold text-gray-900">Data Sources</h1>
              </div>
            </div>
            <nav className="flex items-center gap-6 text-sm font-medium">
              <Link href="/enrollment" className="text-gray-600 hover:text-gray-900">Enrollment</Link>
              <Link href="/stars" className="text-gray-600 hover:text-gray-900">Stars</Link>
              <Link href="/risk-scores" className="text-gray-600 hover:text-gray-900">Risk Scores</Link>
            </nav>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-6 py-8">
        {/* Intro */}
        <div className="mb-8">
          <p className="text-gray-600 max-w-3xl">
            All data in this platform comes from publicly available CMS (Centers for Medicare & Medicaid Services) files. 
            Below are the source files we use, with links to download raw data directly from CMS or from our processed exports.
          </p>
        </div>

        {/* Data Source Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {dataSources.map((source) => {
            const colors = colorClasses[source.color];
            return (
              <div
                key={source.id}
                className={`bg-white rounded-xl border ${colors.border} shadow-sm overflow-hidden`}
              >
                {/* Card Header */}
                <div className={`${colors.light} px-5 py-4 border-b ${colors.border}`}>
                  <div className="flex items-center gap-3">
                    <div className={`${colors.bg} text-white p-2 rounded-lg`}>
                      {source.icon}
                    </div>
                    <div>
                      <h2 className="font-semibold text-gray-900">{source.name}</h2>
                    </div>
                  </div>
                </div>

                {/* Card Body */}
                <div className="p-5">
                  <p className="text-sm text-gray-600 mb-4">{source.description}</p>

                  {/* Files */}
                  <div className="space-y-3">
                    {source.files.map((file, idx) => (
                      <div
                        key={idx}
                        className="flex items-center justify-between p-3 bg-gray-50 rounded-lg"
                      >
                        <div className="flex-1 min-w-0">
                          <div className="font-medium text-gray-900 text-sm">{file.name}</div>
                          <div className="text-xs text-gray-500 truncate">{file.description}</div>
                          <div className="text-xs text-gray-400 mt-0.5">{file.years}</div>
                        </div>
                        <div className="flex items-center gap-2 ml-3">
                          {file.downloadUrl && (
                            <a
                              href={file.downloadUrl}
                              className="flex items-center gap-1.5 px-3 py-1.5 bg-green-600 text-white rounded-lg hover:bg-green-700 transition-colors text-xs font-medium"
                              title="Download processed data"
                            >
                              <Download className="w-3.5 h-3.5" />
                              Export
                            </a>
                          )}
                          {file.cmsUrl && (
                            <a
                              href={file.cmsUrl}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors text-xs font-medium"
                              title="View on CMS.gov"
                            >
                              <ExternalLink className="w-3.5 h-3.5" />
                              CMS
                            </a>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            );
          })}
        </div>

        {/* Footer Note */}
        <div className="mt-12 p-6 bg-amber-50 border border-amber-200 rounded-xl">
          <h3 className="font-semibold text-amber-900 mb-2">Data Update Schedule</h3>
          <p className="text-sm text-amber-800">
            CMS releases most data monthly (enrollment) or annually (star ratings, risk scores). 
            Our platform typically updates within 24-48 hours of new CMS releases. 
            Star ratings are released in October for the following payment year.
          </p>
        </div>
      </main>
    </div>
  );
}
