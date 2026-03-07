"use client";

import { useState, useEffect } from "react";
import { Database, Download, FileText, Users, Star, TrendingUp, AlertTriangle, X, MapPin, ArrowRight, BookOpen, FileSearch, ScrollText, DollarSign, Activity, Cog } from "lucide-react";
import { useQuery } from "@tanstack/react-query";

const API_BASE = (process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000").replace(/\/$/, "");

// CMS Document types
interface CMSDocument {
  year: number;
  name: string;
  type: string;
  key: string;
  size_mb: number;
}

interface DocumentsResponse {
  documents: {
    rate_notices: {
      advance: CMSDocument[];
      final: CMSDocument[];
    };
    technical_notes: {
      stars: CMSDocument[];
    };
    stars_docs: {
      rate_announcements: CMSDocument[];
      cai_supplements: CMSDocument[];
      fact_sheets: CMSDocument[];
    };
    hcc_docs: {
      model: CMSDocument[];
    };
  };
  total_count: number;
}

interface DataSourceConfig {
  id: string;
  name: string;
  shortName: string;
  description: string;
  details: string[];
  icon: React.ReactNode;
  color: string;
  table: string;
  years: number[];
  fileType: string;
}

const generateYears = (start: number, end: number) => 
  Array.from({ length: end - start + 1 }, (_, i) => end - i);

const dataSources: DataSourceConfig[] = [
  {
    id: "cpsc",
    name: "CPSC Enrollment",
    shortName: "CPSC",
    description: "County-level enrollment with geographic detail",
    details: [
      "Contract-Plan-State-County level",
      "Monthly snapshots",
      "Some values suppressed (<10 enrollees)",
      "Includes plan type & organization"
    ],
    icon: <MapPin className="w-7 h-7" />,
    color: "blue",
    table: "cpsc_enrollment",
    years: generateYears(2013, 2025),
    fileType: "ZIP (CSV)",
  },
  {
    id: "enrollment-by-plan",
    name: "Monthly Enrollment",
    shortName: "Enrollment",
    description: "Complete enrollment at contract-plan level",
    details: [
      "Contract + Plan level detail",
      "No geographic suppression",
      "Parent organization included",
      "Most complete enrollment source"
    ],
    icon: <Users className="w-7 h-7" />,
    color: "green",
    table: "enrollment_by_plan",
    years: generateYears(2007, 2025),
    fileType: "ZIP (CSV + Excel)",
  },
  {
    id: "snp",
    name: "SNP Classification",
    shortName: "SNP",
    description: "Special Needs Plan type identification",
    details: [
      "D-SNP (Dual Eligible)",
      "C-SNP (Chronic Condition)",
      "I-SNP (Institutional)",
      "State-level enrollment"
    ],
    icon: <AlertTriangle className="w-7 h-7" />,
    color: "purple",
    table: "snp_enrollment",
    years: generateYears(2007, 2024),
    fileType: "ZIP (Excel + PDF)",
  },
  {
    id: "stars",
    name: "Star Ratings",
    shortName: "Stars",
    description: "Quality ratings and measure performance",
    details: [
      "Overall star ratings (1-5)",
      "Individual measure scores",
      "HEDIS & CAHPS measures",
      "Technical specifications"
    ],
    icon: <Star className="w-7 h-7" />,
    color: "yellow",
    table: "stars_overall",
    years: generateYears(2007, 2026),
    fileType: "ZIP (CSV + Excel)",
  },
  {
    id: "risk-scores",
    name: "Risk Scores",
    shortName: "Risk",
    description: "Plan payment and risk adjustment data",
    details: [
      "Part C & D risk scores",
      "Contract and plan level",
      "County-level breakdowns",
      "Payment reconciliation"
    ],
    icon: <TrendingUp className="w-7 h-7" />,
    color: "red",
    table: "risk_scores",
    years: generateYears(2006, 2024),
    fileType: "ZIP (Excel + PDF)",
  },
  {
    id: "crosswalk",
    name: "Contract Crosswalk",
    shortName: "Crosswalk",
    description: "Track contract changes over time",
    details: [
      "Contract ID changes",
      "Mergers & acquisitions",
      "Plan consolidations",
      "Historical mapping"
    ],
    icon: <Database className="w-7 h-7" />,
    color: "slate",
    table: "crosswalk",
    years: generateYears(2006, 2025),
    fileType: "ZIP (Excel + TXT)",
  },
  {
    id: "ratebook",
    name: "County Ratebook",
    shortName: "Ratebook",
    description: "County-level MA benchmark payment rates",
    details: [
      "$/month payment rates by county",
      "Aged, disabled, ESRD rates",
      "Benchmark quartiles",
      "Quality bonus eligible rates"
    ],
    icon: <DollarSign className="w-7 h-7" />,
    color: "teal",
    table: "ratebook",
    years: generateYears(2016, 2026),
    fileType: "ZIP (Excel)",
  },
];

const colorConfig: Record<string, { 
  bg: string; 
  bgHover: string;
  border: string; 
  text: string; 
  light: string;
  icon: string;
  ring: string;
}> = {
  blue: { bg: "bg-blue-500", bgHover: "hover:bg-blue-600", border: "border-blue-200", text: "text-blue-600", light: "bg-blue-50", icon: "bg-blue-100", ring: "ring-blue-500" },
  green: { bg: "bg-green-500", bgHover: "hover:bg-green-600", border: "border-green-200", text: "text-green-600", light: "bg-green-50", icon: "bg-green-100", ring: "ring-green-500" },
  purple: { bg: "bg-purple-500", bgHover: "hover:bg-purple-600", border: "border-purple-200", text: "text-purple-600", light: "bg-purple-50", icon: "bg-purple-100", ring: "ring-purple-500" },
  yellow: { bg: "bg-amber-500", bgHover: "hover:bg-amber-600", border: "border-amber-200", text: "text-amber-600", light: "bg-amber-50", icon: "bg-amber-100", ring: "ring-amber-500" },
  red: { bg: "bg-red-500", bgHover: "hover:bg-red-600", border: "border-red-200", text: "text-red-600", light: "bg-red-50", icon: "bg-red-100", ring: "ring-red-500" },
  slate: { bg: "bg-slate-500", bgHover: "hover:bg-slate-600", border: "border-slate-200", text: "text-slate-600", light: "bg-slate-50", icon: "bg-slate-100", ring: "ring-slate-500" },
  teal: { bg: "bg-teal-500", bgHover: "hover:bg-teal-600", border: "border-teal-200", text: "text-teal-600", light: "bg-teal-50", icon: "bg-teal-100", ring: "ring-teal-500" },
};

function getDownloadUrl(table: string, year: number): string {
  switch (table) {
    case "cpsc_enrollment":
      return `${API_BASE}/api/data-sources/cpsc?year=${year}&month=12&format=raw`;
    case "enrollment_by_plan":
      return `${API_BASE}/api/data-sources/enrollment?year=${year}&month=12&format=raw`;
    case "snp_enrollment":
      return `${API_BASE}/api/data-sources/snp?year=${year}&month=12&format=raw`;
    case "stars_overall":
      return `${API_BASE}/api/data-sources/stars?year=${year}&format=raw`;
    case "risk_scores":
      return `${API_BASE}/api/data-sources/risk-scores?year=${year}&format=raw`;
    case "crosswalk":
      return `${API_BASE}/api/data-sources/crosswalk?year=${year}&format=raw`;
    case "ratebook":
      return `${API_BASE}/api/data-sources/ratebook?year=${year}&format=raw`;
    default:
      return "#";
  }
}

export default function DataSourcesPage() {
  const [selectedSource, setSelectedSource] = useState<DataSourceConfig | null>(null);
  const [selectedDocCategory, setSelectedDocCategory] = useState<string | null>(null);
  const [downloading, setDownloading] = useState<string | null>(null);

  // Fetch CMS documents list
  const { data: documentsData } = useQuery<DocumentsResponse>({
    queryKey: ["cms-documents"],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/api/documents/list`);
      if (!res.ok) throw new Error("Failed to fetch documents");
      return res.json();
    },
  });

  const handleDownload = (source: DataSourceConfig, year: number) => {
    const key = `${source.id}-${year}`;
    setDownloading(key);
    
    const url = getDownloadUrl(source.table, year);
    window.open(url, '_blank');
    
    setTimeout(() => setDownloading(null), 2000);
  };

  const handleDocumentDownload = (docType: string, year: number) => {
    const key = `${docType}-${year}`;
    setDownloading(key);
    window.open(`${API_BASE}/api/documents/download/${docType}/${year}`, '_blank');
    setTimeout(() => setDownloading(null), 2000);
  };

  return (
    <div className="max-w-7xl mx-auto px-4 py-6 min-h-screen bg-slate-50">

      {/* Main Content */}
      <main className="max-w-6xl mx-auto px-6 py-10">
        {/* Page Title */}
        <div className="mb-10">
          <h2 className="text-2xl font-bold text-slate-900 mb-2">Data Sources & Documents</h2>
          <p className="text-slate-500">
            Download raw CMS data files and policy documents including rate notices and technical notes.
          </p>
        </div>

        {/* Cards Grid - 3 per row */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {dataSources.map((source) => {
            const colors = colorConfig[source.color];
            
            return (
              <button
                key={source.id}
                onClick={() => setSelectedSource(source)}
                className={`
                  bg-white rounded-2xl border border-slate-200 p-6 text-left
                  hover:shadow-lg hover:border-slate-300 hover:scale-[1.02]
                  transition-all duration-200 ease-out
                  focus:outline-none focus:ring-2 ${colors.ring} focus:ring-offset-2
                `}
              >
                {/* Icon */}
                <div className={`${colors.icon} ${colors.text} w-14 h-14 rounded-xl flex items-center justify-center mb-4`}>
                  {source.icon}
                </div>

                {/* Title & Description */}
                <h3 className="text-lg font-semibold text-slate-900 mb-1">{source.name}</h3>
                <p className="text-sm text-slate-500 mb-4">{source.description}</p>

                {/* Meta Info */}
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <span className={`text-xs font-medium px-2 py-1 rounded-full ${colors.light} ${colors.text}`}>
                      {source.years.length} years
                    </span>
                    <span className="text-xs text-slate-400">
                      {source.years[source.years.length - 1]}–{source.years[0]}
                    </span>
                  </div>
                  <ArrowRight className="w-4 h-4 text-slate-300" />
                </div>
              </button>
            );
          })}
        </div>

        {/* CMS Documents Section */}
        <div className="mt-16 mb-10">
          <h2 className="text-2xl font-bold text-slate-900 mb-2">CMS Policy Documents</h2>
          <p className="text-slate-500">
            Official CMS publications including rate notices, technical specifications, and methodology documents.
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mb-12">
          {/* Rate Notices - Advance */}
          <button
            onClick={() => setSelectedDocCategory('rate_notice_advance')}
            className="bg-white rounded-2xl border border-slate-200 p-6 text-left hover:shadow-lg hover:border-slate-300 hover:scale-[1.02] transition-all duration-200 ease-out focus:outline-none focus:ring-2 focus:ring-emerald-500 focus:ring-offset-2"
          >
            <div className="bg-emerald-100 text-emerald-600 w-14 h-14 rounded-xl flex items-center justify-center mb-4">
              <ScrollText className="w-7 h-7" />
            </div>
            <h3 className="text-lg font-semibold text-slate-900 mb-1">Advance Rate Notices</h3>
            <p className="text-sm text-slate-500 mb-4">Proposed MA payment rates released in February</p>
            <div className="flex items-center justify-between">
              <span className="text-xs font-medium px-2 py-1 rounded-full bg-emerald-50 text-emerald-600">
                {documentsData?.documents.rate_notices.advance.length || 0} years
              </span>
              <ArrowRight className="w-4 h-4 text-slate-300" />
            </div>
          </button>

          {/* Rate Notices - Final */}
          <button
            onClick={() => setSelectedDocCategory('rate_notice_final')}
            className="bg-white rounded-2xl border border-slate-200 p-6 text-left hover:shadow-lg hover:border-slate-300 hover:scale-[1.02] transition-all duration-200 ease-out focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2"
          >
            <div className="bg-blue-100 text-blue-600 w-14 h-14 rounded-xl flex items-center justify-center mb-4">
              <FileText className="w-7 h-7" />
            </div>
            <h3 className="text-lg font-semibold text-slate-900 mb-1">Final Rate Notices</h3>
            <p className="text-sm text-slate-500 mb-4">Finalized MA payment rates released in April</p>
            <div className="flex items-center justify-between">
              <span className="text-xs font-medium px-2 py-1 rounded-full bg-blue-50 text-blue-600">
                {documentsData?.documents.rate_notices.final.length || 0} years
              </span>
              <ArrowRight className="w-4 h-4 text-slate-300" />
            </div>
          </button>

          {/* Technical Notes - Stars */}
          <button
            onClick={() => setSelectedDocCategory('tech_notes_stars')}
            className="bg-white rounded-2xl border border-slate-200 p-6 text-left hover:shadow-lg hover:border-slate-300 hover:scale-[1.02] transition-all duration-200 ease-out focus:outline-none focus:ring-2 focus:ring-amber-500 focus:ring-offset-2"
          >
            <div className="bg-amber-100 text-amber-600 w-14 h-14 rounded-xl flex items-center justify-center mb-4">
              <BookOpen className="w-7 h-7" />
            </div>
            <h3 className="text-lg font-semibold text-slate-900 mb-1">Star Ratings Technical Notes</h3>
            <p className="text-sm text-slate-500 mb-4">Methodology and measure specifications</p>
            <div className="flex items-center justify-between">
              <span className="text-xs font-medium px-2 py-1 rounded-full bg-amber-50 text-amber-600">
                {documentsData?.documents.technical_notes.stars.length || 0} years
              </span>
              <ArrowRight className="w-4 h-4 text-slate-300" />
            </div>
          </button>

          {/* Rate Announcements */}
          <button
            onClick={() => setSelectedDocCategory('rate_announcement')}
            className="bg-white rounded-2xl border border-slate-200 p-6 text-left hover:shadow-lg hover:border-slate-300 hover:scale-[1.02] transition-all duration-200 ease-out focus:outline-none focus:ring-2 focus:ring-violet-500 focus:ring-offset-2"
          >
            <div className="bg-violet-100 text-violet-600 w-14 h-14 rounded-xl flex items-center justify-center mb-4">
              <FileSearch className="w-7 h-7" />
            </div>
            <h3 className="text-lg font-semibold text-slate-900 mb-1">Rate Announcements</h3>
            <p className="text-sm text-slate-500 mb-4">Annual MA rate change announcements</p>
            <div className="flex items-center justify-between">
              <span className="text-xs font-medium px-2 py-1 rounded-full bg-violet-50 text-violet-600">
                {documentsData?.documents.stars_docs?.rate_announcements?.length || 0} years
              </span>
              <ArrowRight className="w-4 h-4 text-slate-300" />
            </div>
          </button>

          {/* CAI Supplements */}
          <button
            onClick={() => setSelectedDocCategory('cai_supplement')}
            className="bg-white rounded-2xl border border-slate-200 p-6 text-left hover:shadow-lg hover:border-slate-300 hover:scale-[1.02] transition-all duration-200 ease-out focus:outline-none focus:ring-2 focus:ring-pink-500 focus:ring-offset-2"
          >
            <div className="bg-pink-100 text-pink-600 w-14 h-14 rounded-xl flex items-center justify-center mb-4">
              <Activity className="w-7 h-7" />
            </div>
            <h3 className="text-lg font-semibold text-slate-900 mb-1">CAI Supplements</h3>
            <p className="text-sm text-slate-500 mb-4">Categorical Adjustment Index methodology</p>
            <div className="flex items-center justify-between">
              <span className="text-xs font-medium px-2 py-1 rounded-full bg-pink-50 text-pink-600">
                {documentsData?.documents.stars_docs?.cai_supplements?.length || 0} years
              </span>
              <ArrowRight className="w-4 h-4 text-slate-300" />
            </div>
          </button>

          {/* Star Fact Sheets */}
          <button
            onClick={() => setSelectedDocCategory('star_fact_sheet')}
            className="bg-white rounded-2xl border border-slate-200 p-6 text-left hover:shadow-lg hover:border-slate-300 hover:scale-[1.02] transition-all duration-200 ease-out focus:outline-none focus:ring-2 focus:ring-orange-500 focus:ring-offset-2"
          >
            <div className="bg-orange-100 text-orange-600 w-14 h-14 rounded-xl flex items-center justify-center mb-4">
              <Star className="w-7 h-7" />
            </div>
            <h3 className="text-lg font-semibold text-slate-900 mb-1">Star Fact Sheets</h3>
            <p className="text-sm text-slate-500 mb-4">Summary overview documents</p>
            <div className="flex items-center justify-between">
              <span className="text-xs font-medium px-2 py-1 rounded-full bg-orange-50 text-orange-600">
                {documentsData?.documents.stars_docs?.fact_sheets?.length || 0} years
              </span>
              <ArrowRight className="w-4 h-4 text-slate-300" />
            </div>
          </button>

          {/* HCC Model Documentation */}
          <button
            onClick={() => setSelectedDocCategory('hcc_model')}
            className="bg-white rounded-2xl border border-slate-200 p-6 text-left hover:shadow-lg hover:border-slate-300 hover:scale-[1.02] transition-all duration-200 ease-out focus:outline-none focus:ring-2 focus:ring-cyan-500 focus:ring-offset-2"
          >
            <div className="bg-cyan-100 text-cyan-600 w-14 h-14 rounded-xl flex items-center justify-center mb-4">
              <Cog className="w-7 h-7" />
            </div>
            <h3 className="text-lg font-semibold text-slate-900 mb-1">HCC Model Documentation</h3>
            <p className="text-sm text-slate-500 mb-4">Risk adjustment model specifications</p>
            <div className="flex items-center justify-between">
              <span className="text-xs font-medium px-2 py-1 rounded-full bg-cyan-50 text-cyan-600">
                {documentsData?.documents.hcc_docs?.model?.length || 0} years
              </span>
              <ArrowRight className="w-4 h-4 text-slate-300" />
            </div>
          </button>
        </div>

        {/* Info Footer */}
        <div className="mt-12 bg-white rounded-xl border border-slate-200 p-6">
          <div className="flex items-start gap-4">
            <div className="bg-slate-100 p-2 rounded-lg">
              <Database className="w-5 h-5 text-slate-600" />
            </div>
            <div>
              <h4 className="font-medium text-slate-900 mb-1">About these files</h4>
              <p className="text-sm text-slate-500">
                These are the original CMS data files as published. Most are ZIP archives containing CSV, Excel, 
                and PDF documentation. Enrollment data updates monthly, star ratings release in October, 
                and risk scores update annually after payment reconciliation.
              </p>
            </div>
          </div>
        </div>
      </main>

      {/* Download Modal */}
      {selectedSource && (
        <div 
          className="fixed inset-0 bg-black/50 backdrop-blur-sm z-50 flex items-center justify-center p-4"
          onClick={() => setSelectedSource(null)}
        >
          <div 
            className="bg-white rounded-2xl shadow-2xl max-w-lg w-full max-h-[80vh] overflow-hidden"
            onClick={(e) => e.stopPropagation()}
          >
            {/* Modal Header */}
            <div className={`${colorConfig[selectedSource.color].light} px-6 py-5 border-b border-slate-100`}>
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-4">
                  <div className={`${colorConfig[selectedSource.color].icon} ${colorConfig[selectedSource.color].text} w-12 h-12 rounded-xl flex items-center justify-center`}>
                    {selectedSource.icon}
                  </div>
                  <div>
                    <h3 className="text-lg font-semibold text-slate-900">{selectedSource.name}</h3>
                    <p className="text-sm text-slate-500">{selectedSource.fileType}</p>
                  </div>
                </div>
                <button 
                  onClick={() => setSelectedSource(null)}
                  className="p-2 hover:bg-slate-200 rounded-lg transition-colors"
                >
                  <X className="w-5 h-5 text-slate-400" />
                </button>
              </div>
            </div>

            {/* Modal Body */}
            <div className="p-6 overflow-y-auto max-h-[60vh]">
              {/* Details */}
              <div className="mb-6">
                <h4 className="text-sm font-medium text-slate-700 mb-2">What's included:</h4>
                <ul className="space-y-1.5">
                  {selectedSource.details.map((detail, i) => (
                    <li key={i} className="flex items-center gap-2 text-sm text-slate-600">
                      <div className={`w-1.5 h-1.5 rounded-full ${colorConfig[selectedSource.color].bg}`} />
                      {detail}
                    </li>
                  ))}
                </ul>
              </div>

              {/* Year Selection */}
              <div>
                <h4 className="text-sm font-medium text-slate-700 mb-3">Select year to download:</h4>
                <div className="grid grid-cols-5 gap-2">
                  {selectedSource.years.map((year) => {
                    const isDownloading = downloading === `${selectedSource.id}-${year}`;
                    const colors = colorConfig[selectedSource.color];
                    
                    return (
                      <button
                        key={year}
                        onClick={() => handleDownload(selectedSource, year)}
                        disabled={isDownloading}
                        className={`
                          py-2.5 px-3 rounded-lg text-sm font-medium transition-all
                          ${isDownloading 
                            ? 'bg-green-100 text-green-700 border border-green-300' 
                            : `bg-slate-50 text-slate-700 border border-slate-200 hover:${colors.light} hover:${colors.text} hover:border-${selectedSource.color}-200`
                          }
                        `}
                      >
                        {isDownloading ? (
                          <div className="flex items-center justify-center">
                            <div className="w-4 h-4 border-2 border-green-600 border-t-transparent rounded-full animate-spin" />
                          </div>
                        ) : (
                          year
                        )}
                      </button>
                    );
                  })}
                </div>
              </div>
            </div>

            {/* Modal Footer */}
            <div className="px-6 py-4 bg-slate-50 border-t border-slate-100">
              <p className="text-xs text-slate-400 text-center">
                Files download directly from CMS archives stored in S3
              </p>
            </div>
          </div>
        </div>
      )}

      {/* CMS Documents Modal */}
      {selectedDocCategory && documentsData && (
        <div 
          className="fixed inset-0 bg-black/50 backdrop-blur-sm z-50 flex items-center justify-center p-4"
          onClick={() => setSelectedDocCategory(null)}
        >
          <div 
            className="bg-white rounded-2xl shadow-2xl max-w-lg w-full max-h-[80vh] overflow-hidden"
            onClick={(e) => e.stopPropagation()}
          >
            {/* Modal Header */}
            <div className={`px-6 py-5 border-b border-slate-100 ${
              selectedDocCategory === 'rate_notice_advance' ? 'bg-emerald-50' :
              selectedDocCategory === 'rate_notice_final' ? 'bg-blue-50' :
              selectedDocCategory === 'tech_notes_stars' ? 'bg-amber-50' :
              selectedDocCategory === 'rate_announcement' ? 'bg-violet-50' :
              selectedDocCategory === 'cai_supplement' ? 'bg-pink-50' :
              selectedDocCategory === 'star_fact_sheet' ? 'bg-orange-50' :
              selectedDocCategory === 'hcc_model' ? 'bg-cyan-50' : 'bg-slate-50'
            }`}>
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-4">
                  <div className={`w-12 h-12 rounded-xl flex items-center justify-center ${
                    selectedDocCategory === 'rate_notice_advance' ? 'bg-emerald-100 text-emerald-600' :
                    selectedDocCategory === 'rate_notice_final' ? 'bg-blue-100 text-blue-600' :
                    selectedDocCategory === 'tech_notes_stars' ? 'bg-amber-100 text-amber-600' :
                    selectedDocCategory === 'rate_announcement' ? 'bg-violet-100 text-violet-600' :
                    selectedDocCategory === 'cai_supplement' ? 'bg-pink-100 text-pink-600' :
                    selectedDocCategory === 'star_fact_sheet' ? 'bg-orange-100 text-orange-600' :
                    selectedDocCategory === 'hcc_model' ? 'bg-cyan-100 text-cyan-600' : 'bg-slate-100 text-slate-600'
                  }`}>
                    {selectedDocCategory === 'rate_notice_advance' ? <ScrollText className="w-6 h-6" /> :
                     selectedDocCategory === 'rate_notice_final' ? <FileText className="w-6 h-6" /> :
                     selectedDocCategory === 'tech_notes_stars' ? <BookOpen className="w-6 h-6" /> :
                     selectedDocCategory === 'rate_announcement' ? <FileSearch className="w-6 h-6" /> :
                     selectedDocCategory === 'cai_supplement' ? <Activity className="w-6 h-6" /> :
                     selectedDocCategory === 'star_fact_sheet' ? <Star className="w-6 h-6" /> :
                     selectedDocCategory === 'hcc_model' ? <Cog className="w-6 h-6" /> :
                     <FileText className="w-6 h-6" />}
                  </div>
                  <div>
                    <h3 className="text-lg font-semibold text-slate-900">
                      {selectedDocCategory === 'rate_notice_advance' ? 'Advance Rate Notices' :
                       selectedDocCategory === 'rate_notice_final' ? 'Final Rate Notices' :
                       selectedDocCategory === 'tech_notes_stars' ? 'Star Ratings Technical Notes' :
                       selectedDocCategory === 'rate_announcement' ? 'Rate Announcements' :
                       selectedDocCategory === 'cai_supplement' ? 'CAI Supplements' :
                       selectedDocCategory === 'star_fact_sheet' ? 'Star Fact Sheets' :
                       selectedDocCategory === 'hcc_model' ? 'HCC Model Documentation' :
                       'Documents'}
                    </h3>
                    <p className="text-sm text-slate-500">PDF Documents</p>
                  </div>
                </div>
                <button 
                  onClick={() => setSelectedDocCategory(null)}
                  className="p-2 hover:bg-slate-200 rounded-lg transition-colors"
                >
                  <X className="w-5 h-5 text-slate-400" />
                </button>
              </div>
            </div>

            {/* Modal Body */}
            <div className="p-6 overflow-y-auto max-h-[60vh]">
              <div className="mb-4">
                <h4 className="text-sm font-medium text-slate-700 mb-2">What's included:</h4>
                <ul className="space-y-1.5 text-sm text-slate-600">
                  {selectedDocCategory === 'rate_notice_advance' && (
                    <>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-emerald-500" />Proposed MA growth rates</li>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-emerald-500" />Draft risk adjustment changes</li>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-emerald-500" />Star bonus proposals</li>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-emerald-500" />Part D parameters</li>
                    </>
                  )}
                  {selectedDocCategory === 'rate_notice_final' && (
                    <>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-blue-500" />Final MA growth rates</li>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-blue-500" />Risk adjustment coefficients</li>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-blue-500" />County benchmark rates</li>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-blue-500" />Quality bonus percentages</li>
                    </>
                  )}
                  {selectedDocCategory === 'tech_notes_stars' && (
                    <>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-amber-500" />Measure specifications</li>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-amber-500" />Cut point methodology</li>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-amber-500" />Weight assignments</li>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-amber-500" />Data source details</li>
                    </>
                  )}
                  {selectedDocCategory === 'rate_announcement' && (
                    <>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-violet-500" />Annual rate change summary</li>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-violet-500" />Growth rate highlights</li>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-violet-500" />Policy impact overview</li>
                    </>
                  )}
                  {selectedDocCategory === 'cai_supplement' && (
                    <>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-pink-500" />CAI methodology</li>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-pink-500" />Adjustment calculations</li>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-pink-500" />Star rating impacts</li>
                    </>
                  )}
                  {selectedDocCategory === 'star_fact_sheet' && (
                    <>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-orange-500" />Key rating highlights</li>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-orange-500" />Industry summary</li>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-orange-500" />Year-over-year changes</li>
                    </>
                  )}
                  {selectedDocCategory === 'hcc_model' && (
                    <>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-cyan-500" />HCC coefficients</li>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-cyan-500" />Model specifications</li>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-cyan-500" />Risk adjustment factors</li>
                      <li className="flex items-center gap-2"><div className="w-1.5 h-1.5 rounded-full bg-cyan-500" />Normalization factors</li>
                    </>
                  )}
                </ul>
              </div>

              {/* Year Selection */}
              <div>
                <h4 className="text-sm font-medium text-slate-700 mb-3">Select year to download:</h4>
                <div className="grid grid-cols-4 gap-2">
                  {(selectedDocCategory === 'rate_notice_advance' 
                    ? documentsData.documents.rate_notices.advance
                    : selectedDocCategory === 'rate_notice_final'
                    ? documentsData.documents.rate_notices.final
                    : selectedDocCategory === 'tech_notes_stars'
                    ? documentsData.documents.technical_notes.stars
                    : selectedDocCategory === 'rate_announcement'
                    ? documentsData.documents.stars_docs?.rate_announcements || []
                    : selectedDocCategory === 'cai_supplement'
                    ? documentsData.documents.stars_docs?.cai_supplements || []
                    : selectedDocCategory === 'star_fact_sheet'
                    ? documentsData.documents.stars_docs?.fact_sheets || []
                    : selectedDocCategory === 'hcc_model'
                    ? documentsData.documents.hcc_docs?.model || []
                    : []
                  ).map((doc) => {
                    const isDownloading = downloading === `${doc.type}-${doc.year}`;
                    
                    return (
                      <button
                        key={doc.year}
                        onClick={() => handleDocumentDownload(selectedDocCategory, doc.year)}
                        disabled={isDownloading}
                        className={`py-2.5 px-3 rounded-lg text-sm font-medium transition-all ${
                          isDownloading 
                            ? 'bg-green-100 text-green-700 border border-green-300' 
                            : 'bg-slate-50 text-slate-700 border border-slate-200 hover:bg-slate-100'
                        }`}
                      >
                        {isDownloading ? (
                          <div className="flex items-center justify-center">
                            <div className="w-4 h-4 border-2 border-green-600 border-t-transparent rounded-full animate-spin" />
                          </div>
                        ) : (
                          <div className="text-center">
                            <div>{doc.year}</div>
                            <div className="text-xs text-slate-400">{doc.size_mb}MB</div>
                          </div>
                        )}
                      </button>
                    );
                  })}
                </div>
              </div>
            </div>

            {/* Modal Footer */}
            <div className="px-6 py-4 bg-slate-50 border-t border-slate-100">
              <p className="text-xs text-slate-400 text-center">
                Original CMS PDF documents • Use in Ask AI for context-aware questions
              </p>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
