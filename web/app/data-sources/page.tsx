"use client";

import { useState } from "react";
import Link from "next/link";
import { Home, Database, Download, FileText, Users, Star, TrendingUp, AlertTriangle, ChevronRight, ChevronDown, X } from "lucide-react";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

interface DataSourceConfig {
  id: string;
  name: string;
  description: string;
  icon: React.ReactNode;
  color: string;
  table: string;
  years: number[];
  columns?: string[];
}

const currentYear = 2026;
const generateYears = (start: number, end: number = currentYear) => 
  Array.from({ length: end - start + 1 }, (_, i) => end - i);

const dataSources: DataSourceConfig[] = [
  {
    id: "cpsc",
    name: "CPSC Enrollment",
    description: "Monthly Enrollment by Contract-Plan-State-County. The most granular enrollment data with geographic detail. Some values suppressed for HIPAA (<10 enrollees).",
    icon: <Users className="w-6 h-6" />,
    color: "blue",
    table: "cpsc_enrollment",
    years: generateYears(2007),
  },
  {
    id: "enrollment-by-plan",
    name: "Enrollment by Plan",
    description: "Monthly enrollment at contract-plan level with parent organization. No geographic detail but includes all enrollment (no suppression).",
    icon: <FileText className="w-6 h-6" />,
    color: "green",
    table: "enrollment_by_plan",
    years: generateYears(2013),
  },
  {
    id: "snp",
    name: "Special Needs Plans (SNP)",
    description: "Enrollment data for D-SNP (Dual), C-SNP (Chronic), and I-SNP (Institutional) plans with state-level detail.",
    icon: <AlertTriangle className="w-6 h-6" />,
    color: "purple",
    table: "snp_enrollment",
    years: generateYears(2010),
  },
  {
    id: "stars-overall",
    name: "Star Ratings - Overall",
    description: "Overall star ratings (1-5 stars) for each MA contract. Used for quality bonus payments and enrollment decisions.",
    icon: <Star className="w-6 h-6" />,
    color: "yellow",
    table: "stars_overall",
    years: generateYears(2009),
  },
  {
    id: "stars-measures",
    name: "Star Ratings - Measures",
    description: "Performance on individual quality measures (HEDIS, CAHPS, HOS). ~40 measures for Part C, ~15 for Part D.",
    icon: <Star className="w-6 h-6" />,
    color: "orange",
    table: "stars_measures",
    years: generateYears(2015),
  },
  {
    id: "cutpoints",
    name: "Star Cutpoints",
    description: "Performance thresholds that determine star ratings. Updated annually by CMS. Critical for predicting future ratings.",
    icon: <TrendingUp className="w-6 h-6" />,
    color: "teal",
    table: "cutpoints",
    years: generateYears(2014),
  },
  {
    id: "risk-scores",
    name: "Risk Adjustment Scores",
    description: "CMS risk scores by contract/plan. Higher scores = sicker population = higher payments. Critical for understanding plan economics.",
    icon: <TrendingUp className="w-6 h-6" />,
    color: "red",
    table: "risk_scores",
    years: generateYears(2007),
  },
  {
    id: "crosswalk",
    name: "Contract Crosswalk",
    description: "Tracks contract ID changes over time (mergers, acquisitions, rebranding). Essential for longitudinal analysis.",
    icon: <Database className="w-6 h-6" />,
    color: "gray",
    table: "crosswalk",
    years: generateYears(2007),
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

function getDownloadUrl(table: string, year: number): string {
  switch (table) {
    case "cpsc_enrollment":
      return `${API_BASE}/api/data-sources/cpsc?year=${year}&format=xlsx`;
    case "enrollment_by_plan":
      return `${API_BASE}/api/data-sources/enrollment?year=${year}&format=xlsx`;
    case "snp_enrollment":
      return `${API_BASE}/api/data-sources/snp?year=${year}&format=xlsx`;
    case "stars_overall":
      return `${API_BASE}/api/stars/export?year=${year}&format=xlsx`;
    case "stars_measures":
      return `${API_BASE}/api/stars/measure-export?year=${year}&format=xlsx`;
    case "cutpoints":
      return `${API_BASE}/api/stars/cutpoints-export?year=${year}&format=xlsx`;
    case "risk_scores":
      return `${API_BASE}/api/v4/risk-scores/export?year=${year}&format=xlsx`;
    case "crosswalk":
      return `${API_BASE}/api/data-sources/crosswalk?year=${year}&format=xlsx`;
    default:
      return "#";
  }
}

export default function DataSourcesPage() {
  const [expandedSource, setExpandedSource] = useState<string | null>(null);
  const [downloading, setDownloading] = useState<string | null>(null);

  const handleDownload = async (source: DataSourceConfig, year: number) => {
    const key = `${source.id}-${year}`;
    setDownloading(key);
    
    try {
      const url = getDownloadUrl(source.table, year);
      window.open(url, '_blank');
    } finally {
      setTimeout(() => setDownloading(null), 1000);
    }
  };

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
            Download processed CMS data directly. Click a data source to see available years, then click a year to download as Excel.
          </p>
        </div>

        {/* Data Source Cards */}
        <div className="space-y-4">
          {dataSources.map((source) => {
            const colors = colorClasses[source.color];
            const isExpanded = expandedSource === source.id;

            return (
              <div
                key={source.id}
                className={`bg-white rounded-xl border ${colors.border} shadow-sm overflow-hidden transition-all`}
              >
                {/* Card Header - Clickable */}
                <button
                  onClick={() => setExpandedSource(isExpanded ? null : source.id)}
                  className={`w-full ${colors.light} px-5 py-4 flex items-center justify-between hover:opacity-90 transition-opacity`}
                >
                  <div className="flex items-center gap-3">
                    <div className={`${colors.bg} text-white p-2 rounded-lg`}>
                      {source.icon}
                    </div>
                    <div className="text-left">
                      <h2 className="font-semibold text-gray-900">{source.name}</h2>
                      <p className="text-sm text-gray-500">
                        {source.years[source.years.length - 1]} - {source.years[0]} • {source.years.length} years
                      </p>
                    </div>
                  </div>
                  <ChevronDown className={`w-5 h-5 text-gray-400 transition-transform ${isExpanded ? 'rotate-180' : ''}`} />
                </button>

                {/* Expanded Content */}
                {isExpanded && (
                  <div className="p-5 border-t border-gray-100">
                    <p className="text-sm text-gray-600 mb-4">{source.description}</p>
                    
                    {/* Year Grid */}
                    <div className="grid grid-cols-4 sm:grid-cols-6 md:grid-cols-8 lg:grid-cols-10 gap-2">
                      {source.years.map((year) => {
                        const isDownloading = downloading === `${source.id}-${year}`;
                        return (
                          <button
                            key={year}
                            onClick={() => handleDownload(source, year)}
                            disabled={isDownloading}
                            className={`
                              flex items-center justify-center gap-1.5 px-3 py-2 rounded-lg text-sm font-medium
                              transition-all border
                              ${isDownloading
                                ? 'bg-green-100 border-green-300 text-green-700'
                                : `${colors.light} ${colors.border} ${colors.text} hover:opacity-80`
                              }
                            `}
                          >
                            {isDownloading ? (
                              <div className="w-4 h-4 border-2 border-green-600 border-t-transparent rounded-full animate-spin" />
                            ) : (
                              <Download className="w-3.5 h-3.5" />
                            )}
                            {year}
                          </button>
                        );
                      })}
                    </div>

                    {/* Download All Button */}
                    <div className="mt-4 pt-4 border-t border-gray-100 flex items-center justify-between">
                      <span className="text-xs text-gray-400">
                        Click a year to download that year&apos;s data as Excel
                      </span>
                      <button
                        onClick={() => {
                          const url = getDownloadUrl(source.table, 0).replace('year=0', 'all=true');
                          window.open(url, '_blank');
                        }}
                        className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-900 text-white rounded-lg hover:bg-gray-800 transition-colors text-sm font-medium"
                      >
                        <Download className="w-4 h-4" />
                        Download All Years
                      </button>
                    </div>
                  </div>
                )}
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
